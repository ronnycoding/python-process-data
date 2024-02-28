CREATE TABLE IF NOT EXISTS Data (
  ID INT AUTO_INCREMENT PRIMARY KEY,
  StringA VARCHAR(255),
  StringB VARCHAR(255),
  Flag BOOLEAN NOT NULL
);

-- Seed database
INSERT INTO Data (StringA, StringB, Flag) VALUES (
  'Hello', 'World', TRUE
), (
  'Alicia', 'Keys', FALSE
);