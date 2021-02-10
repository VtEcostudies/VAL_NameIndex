select * from val_species where "gbifId"=100040629;
select * from val_species where "gbifId"=135229298;
select * from val_species where "gbifId"=100052486;
select * from val_species where "gbifId"=100056075;
select * from val_species where "gbifId"=100057616;
select * from val_species where "gbifId"=135229340;
select * from val_species where "gbifId"=132928401;
select * from val_species where "kingdomId"='132928401'; --bad kingdomId, no GBIF results

select * from val_species where "taxonRank"='kingdom';
update val_species set "kingdomId"='1' where "kingdomId"='132928401'; --fix one

select * from val_species where "kingdom"='Animalia' and "kingdomId" != '1'; --60
update val_species set "kingdomId"='1' where "kingdom"='Animalia' and "kingdomId" != '1'; --60
select * from val_vernacular where "taxonId" in (select "kingdomId" from val_species where "kingdom"='Animalia'); --1?
select * from val_vernacular where "scientificName"='Animalia' and "taxonId" != '1'; --4
delete from val_vernacular where "scientificName"='Animalia' and "taxonId" != '1'; --4
select * from val_species where "scientificName"='Animalia' and "gbifId" != 1; --5
delete from val_species where "scientificName"='Animalia' and "gbifId" != 1; --5

select * from val_species where "kingdom"='Plantae' and "kingdomId" != '6'; --50
update val_species set "kingdomId"='6' where "kingdom"='Plantae' and "kingdomId" != '6'; --50
select * from val_species where "kingdom"='Metazoa'; --
update val_species set "kingdomId"='6' where "kingdom"='Plantae' and "kingdomId" != '6'; --50

select * from val_vernacular where "scientificName"='Plantae' and "taxonId" != '6'; --4
delete from val_vernacular where "scientificName"='Plantae' and "taxonId" != '6'; --4
select * from val_species where "scientificName"='Plantae' and "gbifId" != 6; --4
delete from val_species where "scientificName"='Plantae' and "gbifId" != 6; --4