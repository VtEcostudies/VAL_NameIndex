DROP TABLE IF EXISTS val_status_level;
DROP TABLE IF EXISTS val_status_scope;
DROP TABLE IF EXISTS val_status_qualifier;

CREATE TABLE val_status_level
(
"statusLevelId" VARCHAR UNIQUE PRIMARY KEY,
"statusLevelName" VARCHAR NOT NULL,
"statusLevelDesc" VARCHAR
);
ALTER TABLE val_status_level OWNER to VAL;

CREATE TABLE val_status_scope
(
"statusScopeId" VARCHAR UNIQUE PRIMARY KEY,
"statusScopeName" VARCHAR NOT NULL,
"statusScopeDesc" VARCHAR
);
ALTER TABLE val_status_scope OWNER to VAL;

CREATE TABLE val_status_qualifier
(
"statusQualifierId" VARCHAR UNIQUE PRIMARY KEY,
"statusQualifierName" VARCHAR NOT NULL,
"statusQualifierDesc" VARCHAR
);
ALTER TABLE val_status_scope OWNER to VAL;

INSERT INTO val_status_level ("statusLevelId","statusLevelName","statusLevelDesc") VALUES
('X','Presumed Extirpated','Species or community is believed to be extirpated from the nation or state/province. Not located despite intensive searches of historical sites and other appropriate habitat, and virtually no likelihood that it will be rediscovered'),
('H','Possibly Extirpated','(Historical)—Species or community occurred historically in the nation or state/province, and there is some possibility that it may be rediscovered. Its presence may not have been verified in the past 20-40 years. A species or community could become NH or SH without such a 20-40 year delay if the only known occurrences in a nation or state/province were destroyed or if it had been extensively and unsuccessfully looked for. The NH or SH rank is reserved for species or communities for which some effort has been made to relocate occurrences, rather than simply using this status for all elements not known from verified extant occurrences.'),
('1','Critically Imperiled','Critically imperiled in the nation or state/province because of extreme rarity (often 5 or fewer occurrences) or because of some factor(s) such as very steep declines making it especially vulnerable to extirpation from the state/province.'),
('2','Imperiled','Imperiled in the nation or state/province because of rarity due to very restricted range, very few populations (often 20 or fewer), steep declines, or other factors making it very vulnerable to extirpation from the nation or state/province.'),
('3','Vulnerable','Vulnerable in the nation or state/province due to a restricted range, relatively few populations (often 80 or fewer), recent and widespread declines, or other factors making it vulnerable to extirpation.'),
('4','Apparently Secure','Uncommon but not rare; some cause for long-term concern due to declines or other factors.'),
('5','Secure','Common, widespread, and abundant in the nation or state/province.'),
('NR','Unranked','Nation or state/province conservation status not yet assessed.'),
('U','Unrankable','Currently unrankable due to lack of information or due to substantially conflicting information about status or trends.'),
('NA','Not Applicable','A conservation status rank is not applicable because the species is not a suitable target for conservation activities.');

INSERT INTO val_status_scope ("statusScopeId","statusScopeName","statusScopeDesc") VALUES
('S','Subnational','S-rank. The term "subnational" refers to state or province-level jurisdictions (e.g., California, Ontario).'),
('N','National','N-rank. National jurisdiction. The US Federal Government.');

INSERT INTO val_status_qualifier("statusQualifierId","statusQualifierName","statusQualifierDesc") VALUES
('B','Breeding','Conservation status refers to the breeding population of the species in the nation or state/province.'),
('N','Nonbreeding','Conservation status refers to the non-breeding population of the species in the nation or state/province.'),
('M','Migrant','Migrant species occurring regularly on migration at particular staging areas or concentration spots where the species might warrant conservation attention. Conservation status refers to the aggregating transient population of the species in the nation or state/province.'),
('?','Inexact or Uncertain','Denotes inexact or uncertain numeric rank. (The ? qualifies the character immediately preceding it in the N- or S-rank.)');
