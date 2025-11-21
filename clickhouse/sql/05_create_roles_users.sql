CREATE ROLE IF NOT EXISTS analyst_full;
CREATE ROLE IF NOT EXISTS analyst_limited;

CREATE USER IF NOT EXISTS user_full IDENTIFIED BY 'full123';
CREATE USER IF NOT EXISTS user_limited IDENTIFIED BY 'limited123';

GRANT analyst_full TO user_full;
GRANT analyst_limited to user_limited;