DROP TABLE cse532.facilitycertification;

CREATE TABLE cse532.facilitycertification
(
    FacilityID     VARCHAR(16),
    FacilityName   VARCHAR(256),
    Description    VARCHAR(128),
    AttributeType  VARCHAR(64),
    AttributeValue VARCHAR(128),
    MeasureValue   INTEGER,
    County         VARCHAR(16)
);
