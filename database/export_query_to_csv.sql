select * from val_species where "gbifId" IN (4298895, 4402879);

copy (select * from val_species where "gbifId" IN (4298895, 4402879))
to 'C:\Users\jloomis\Documents\VCE\VAL_NameIndex\repo\database\export\val_species_pg_export_example.csv' 
delimiter ','
csv header;