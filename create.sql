
CREATE TABLE country
(
  country_id              INTEGER   NOT NULL,
  country      CHAR(100) NOT NULL
);

CREATE TABLE state
(
  state_id              INTEGER   NOT NULL,
  state      CHAR(100) NOT NULL
);

CREATE TABLE time
(
  time_id              INTEGER   NOT NULL,
  date      DATE NOT NULL
);

CREATE TABLE reason
(
  reason_id              INTEGER   NOT NULL,
  reason      CHAR(100) NOT NULL
);

CREATE TABLE death
(
  death_id             INTEGER   NOT NULL,
  place_id             INTEGER   NOT NULL,
  time_id              INTEGER   NOT NULL,
  reason_id            INTEGER   NOT NULL,
  number_of_deaths     INTEGER,
  is_driver_dead       BOOLEAN,
  is_occupant_dead     BOOLEAN
);

CREATE TABLE place
(
  place_id             INTEGER        NOT NULL ,
  country_id      INTEGER        NOT NULL ,
  state_id       INTEGER        NOT NULL 
);

ALTER TABLE country ADD PRIMARY KEY (country_id);
ALTER TABLE state ADD PRIMARY KEY (state_id);
ALTER TABLE death ADD PRIMARY KEY (death_id);
ALTER TABLE reason ADD PRIMARY KEY (reason_id);
ALTER TABLE time ADD PRIMARY KEY (time_id);
ALTER TABLE place ADD PRIMARY KEY (place_id);

ALTER TABLE death ADD CONSTRAINT FK_Death_Place FOREIGN KEY (place_id) REFERENCES place (place_id);
ALTER TABLE death ADD CONSTRAINT FK_Death_Time FOREIGN KEY (time_id) REFERENCES time (time_id);
ALTER TABLE death ADD CONSTRAINT FK_Death_Reason FOREIGN KEY (reason_id) REFERENCES reason (reason_id);
ALTER TABLE place ADD CONSTRAINT FK_State_Country FOREIGN KEY (state_id) REFERENCES state (state_id);
ALTER TABLE place ADD CONSTRAINT FK_Country_State FOREIGN KEY (country_id) REFERENCES country (country_id);