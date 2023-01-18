DO $$
DECLARE
    reason_id   reason.reason_id%TYPE;
    reason reason.reason%TYPE;

BEGIN
    reason_id := 8;
    reason := 'reason_';
    FOR counter IN 1..10
        LOOP
            INSERT INTO reason (reason_id, reason)
             VALUES (counter + reason_id, reason || counter);
        END LOOP;
END$$;
SELECT * FROM reason;