select * from val_species where "gbifId"=106383006;
select * from val_species where "gbifId"=160127533;
select * from val_species where "gbifId"=155946844;
select * from val_species where "gbifId"=160127571;

delete from val_vernacular where "taxonId"='106383006';
delete from val_species where "taxonId"='106383006';

delete from val_vernacular where "taxonId"='160127533';
delete from val_species where "taxonId"='160127533';

delete from val_vernacular where "taxonId"='155946844';
delete from val_species where "taxonId"='155946844';

delete from val_vernacular where "taxonId"='160127571';
delete from val_species where "taxonId"='160127571';
