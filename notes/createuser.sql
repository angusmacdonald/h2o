DROP USER angus;

CREATE USER angus PASSWORD 'supersecret' ADMIN;

GRANT {SELECT | INSERT | UPDATE | DELETE | ALL} [,...]
ON tableName [,...] TO {PUBLIC | userName | roleName}