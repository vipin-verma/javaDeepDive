SET SERVEROUTPUT ON;

---------------------------------------------------------------------------
-- Step 1: Recreate sequences dynamically based on existing data
---------------------------------------------------------------------------
DECLARE
   l_max_factor NUMBER;
   l_max_attr   NUMBER;
   l_new_factor_start NUMBER;
   l_new_attr_start   NUMBER;
   l_min_start CONSTANT NUMBER := 10;  -- Minimum start value to ensure gap if desired
BEGIN
   -- Get the maximum existing FACTOR_ID
   SELECT NVL(MAX(FACTOR_ID), 0) INTO l_max_factor
     FROM DEV_OAAM.VCRYPT_USERS_FACTORS;

   -- Get the maximum existing F_ATTR_ID
   SELECT NVL(MAX(F_ATTR_ID), 0) INTO l_max_attr
     FROM DEV_OAAM.VCRYPT_USERS_FACTORS_ATTR;

   -- Determine new start values: use GREATEST to ensure at least l_min_start
   l_new_factor_start := GREATEST(l_max_factor + 1, l_min_start);
   l_new_attr_start   := GREATEST(l_max_attr + 1, l_min_start);

   DBMS_OUTPUT.PUT_LINE('Max FACTOR_ID = ' || l_max_factor || ', new FACTOR sequence will start at ' || l_new_factor_start);
   DBMS_OUTPUT.PUT_LINE('Max F_ATTR_ID = ' || l_max_attr || ', new ATTR sequence will start at ' || l_new_attr_start);

   -- Drop and recreate the FACTORS sequence
   BEGIN
      EXECUTE IMMEDIATE 'DROP SEQUENCE DEV_OAAM.SEQ_VCRYPT_USERS_FACTORS';
      DBMS_OUTPUT.PUT_LINE('Dropped sequence DEV_OAAM.SEQ_VCRYPT_USERS_FACTORS.');
   EXCEPTION
      WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('Warning: Could not drop sequence DEV_OAAM.SEQ_VCRYPT_USERS_FACTORS: ' || SQLERRM);
   END;

   EXECUTE IMMEDIATE 'CREATE SEQUENCE DEV_OAAM.SEQ_VCRYPT_USERS_FACTORS START WITH ' || l_new_factor_start || ' INCREMENT BY 1 NOCACHE NOCYCLE';
   DBMS_OUTPUT.PUT_LINE('Created sequence DEV_OAAM.SEQ_VCRYPT_USERS_FACTORS starting at ' || l_new_factor_start);

   -- Drop and recreate the ATTRIBUTES sequence
   BEGIN
      EXECUTE IMMEDIATE 'DROP SEQUENCE DEV_OAAM.SEQ_VCRYPT_USERS_FACTORS_ATTR';
      DBMS_OUTPUT.PUT_LINE('Dropped sequence DEV_OAAM.SEQ_VCRYPT_USERS_FACTORS_ATTR.');
   EXCEPTION
      WHEN OTHERS THEN
         DBMS_OUTPUT.PUT_LINE('Warning: Could not drop sequence DEV_OAAM.SEQ_VCRYPT_USERS_FACTORS_ATTR: ' || SQLERRM);
   END;

   EXECUTE IMMEDIATE 'CREATE SEQUENCE DEV_OAAM.SEQ_VCRYPT_USERS_FACTORS_ATTR START WITH ' || l_new_attr_start || ' INCREMENT BY 1 NOCACHE NOCYCLE';
   DBMS_OUTPUT.PUT_LINE('Created sequence DEV_OAAM.SEQ_VCRYPT_USERS_FACTORS_ATTR starting at ' || l_new_attr_start);

   COMMIT;
EXCEPTION
   WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error during sequence recreation: ' || SQLERRM);
      ROLLBACK;
      RAISE;
END;
/
---------------------------------------------------------------------------
-- Step 2: Main copy block
-- Copy data from source factor 'ChallengeOMAPUSH' to create target factors
-- for each device. (Supports multiple devices per user.)
---------------------------------------------------------------------------

DECLARE
  -- Source factor name (matches the FACTOR column)
  p_source_factor VARCHAR2(256) := 'ChallengeOMAPUSH';

  -- Array of target factors to create
  TYPE factor_array IS TABLE OF VARCHAR2(256);
  p_target_factors factor_array := factor_array(
    'ChallengeOMAPUSHNUMBER',
    'ChallengeOMAPUSHCODEINPUT',
    'ChallengeOMAFIDOPUSH'
  );

  -- Variables corresponding to columns in VCRYPT_USERS_FACTORS
  v_SOURCE_FACTOR_ID  NUMBER(16,0);             -- Source row's FACTOR_ID
  v_USER_ID           NUMBER(16,0);              -- USER_ID
  v_DEVICE_NAME       VARCHAR2(256);       -- DEVICE_NAME
  v_ENABLED           NUMBER(3,0);         -- ENABLED
  v_VALIDATED         NUMBER(3,0);         -- VALIDATED
  v_CREATE_TIME       TIMESTAMP(6);        -- For new rows (CREATE_TIME)
  v_UPDATE_TIME       TIMESTAMP(6);        -- For new rows (UPDATE_TIME)

  -- Variables for new IDs (fetched from sequences)
  v_new_FACTOR_ID  NUMBER(16,0);
  v_new_F_ATTR_ID  NUMBER(16,0);

  -- Variable to check existence of target factor per user/device
  v_factor_exists  NUMBER;

  -- Cursor to get all rows having the source factor
  CURSOR c_source_factors IS
    SELECT FACTOR_ID, USER_ID, DEVICE_NAME, ENABLED, VALIDATED, CREATE_TIME, UPDATE_TIME
    FROM DEV_OAAM.VCRYPT_USERS_FACTORS
    WHERE FACTOR = p_source_factor;

BEGIN
  -- Use current timestamp for new rows
  v_CREATE_TIME := SYSTIMESTAMP;
  v_UPDATE_TIME := v_CREATE_TIME;

  -- Loop over each source row (each represents one user-device record)
  FOR rec IN c_source_factors LOOP
    v_SOURCE_FACTOR_ID := rec.FACTOR_ID;
    v_USER_ID          := rec.USER_ID;
    v_DEVICE_NAME      := rec.DEVICE_NAME;
    v_ENABLED          := rec.ENABLED;
    v_VALIDATED        := rec.VALIDATED;

    -- Loop through each target factor value
    FOR i IN 1..p_target_factors.COUNT LOOP

      -- Check if a target factor already exists for this user and device
      SELECT COUNT(*)
      INTO v_factor_exists
      FROM DEV_OAAM.VCRYPT_USERS_FACTORS
      WHERE USER_ID = v_USER_ID
        AND FACTOR = p_target_factors(i)
        AND DEVICE_NAME = v_DEVICE_NAME;

      IF v_factor_exists = 0 THEN
        -- Get new FACTOR_ID using the sequence
        SELECT DEV_OAAM.SEQ_VCRYPT_USERS_FACTORS.NEXTVAL
          INTO v_new_FACTOR_ID
          FROM DUAL;

        -- Insert new target factor row
        INSERT INTO DEV_OAAM.VCRYPT_USERS_FACTORS (
          FACTOR_ID, USER_ID, FACTOR, DEVICE_NAME, ENABLED, VALIDATED, CREATE_TIME, UPDATE_TIME
        ) VALUES (
          v_new_FACTOR_ID, v_USER_ID, p_target_factors(i), v_DEVICE_NAME, v_ENABLED, v_VALIDATED, v_CREATE_TIME, v_UPDATE_TIME
        );

        -- Copy attributes for the source factor for this device
        FOR attr_rec IN (
          SELECT KEY_NAME, VALUE
          FROM DEV_OAAM.VCRYPT_USERS_FACTORS_ATTR
          WHERE FACTOR_ID = v_SOURCE_FACTOR_ID
        ) LOOP
          SELECT DEV_OAAM.SEQ_VCRYPT_USERS_FACTORS_ATTR.NEXTVAL
            INTO v_new_F_ATTR_ID
            FROM DUAL;

          INSERT INTO DEV_OAAM.VCRYPT_USERS_FACTORS_ATTR (
            F_ATTR_ID, FACTOR_ID, KEY_NAME, VALUE, CREATE_TIME, UPDATE_TIME
          ) VALUES (
            v_new_F_ATTR_ID, v_new_FACTOR_ID, attr_rec.KEY_NAME, attr_rec.VALUE, v_CREATE_TIME, v_UPDATE_TIME
          );
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('Created ' || p_target_factors(i) ||
                             ' for USER_ID ' || v_USER_ID ||
                             ' and DEVICE_NAME ' || v_DEVICE_NAME);
      ELSE
        DBMS_OUTPUT.PUT_LINE('Factor ' || p_target_factors(i) ||
                             ' already exists for USER_ID ' || v_USER_ID ||
                             ' and DEVICE_NAME ' || v_DEVICE_NAME);
      END IF;
    END LOOP;
  END LOOP;

  COMMIT;
  DBMS_OUTPUT.PUT_LINE('Data copy completed successfully.');

EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('Error in copy block: ' || SQLERRM);
END;
/
