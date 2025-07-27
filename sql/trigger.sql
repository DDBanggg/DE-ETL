CREATE TABLE user_log_before(
    user_id BIGINT ,
    login VARCHAR(255) ,
    gravatar_id VARCHAR(255) ,
    url VARCHAR(255) ,
    avatar_url VARCHAR(255),
    state VARCHAR(255),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) PRIMARY KEY
);

CREATE TABLE user_log_after(
    user_id BIGINT,
    login VARCHAR(255) ,
    gravatar_id VARCHAR(255) ,
    url VARCHAR(255) ,
    avatar_url VARCHAR(255),
    state VARCHAR(255),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) PRIMARY KEY
);

DELIMITER //

CREATE TRIGGER before_insert_users
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id, login, gravatar_id, url,avatar_url,state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.url, NEW.avatar_url, "INSERT");
END //

CREATE TRIGGER before_update_users
BEFORE UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id, login, gravatar_id, url,avatar_url,state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.url, OLD.avatar_url, "UPDATE");
END //

CREATE TRIGGER before_delete_users
BEFORE DELETE ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id, login, gravatar_id, url,avatar_url,state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.url, OLD.avatar_url, "DELETE");
END //

CREATE TRIGGER after_update_users
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id,login, gravatar_id,  avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id,NEW.avatar_url ,NEW.url, "UPDATE");
END //

CREATE TRIGGER after_insert_users
AFTER INSERT ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id,login, gravatar_id,  avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id,NEW.avatar_url ,NEW.url, "INSERT");
END //

CREATE TRIGGER after_delete_users
AFTER DELETE ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id,login, gravatar_id,  avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id,OLD.avatar_url ,OLD.url, "DELETE");
END //

DELIMITER ;