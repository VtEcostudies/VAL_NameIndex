copy
(select * from val_species where "createdAt" > '2021-05-01')
to 'C:\Users\jloomis\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\val_species_gbif_update_createdAt_2021-05-11.csv' 
delimiter ','
csv header;