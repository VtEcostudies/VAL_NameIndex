select * from val_species where "class" LIKE 'Equisetopsida%'; --36
select * from val_species where "kingdom" LIKE 'incertae%'; --we already had incertae sedis kingdom

select * from val_species where "class"='Equisetopsida'; --36
update val_species set class='Equisetopsida S. L.' where class='Equisetopsida';
select * from val_species where "scientificName"='Equisetopsida'; --1
update val_species set "scientificName"='Equisetopsida S. L.' where "scientificName"='Equisetopsida';

insert into val_species --create a phylum called 'incertae sedis'
("gbifId","taxonId","scientificName","acceptedNameUsage","acceptedNameUsageId","parentNameUsageId","taxonRank","taxonomicStatus","kingdom","kingdomId","phylum","phylumId")
values
(-2,'-2','incertae sedis','incertae sedis','-2','-1','phylum','doubtful','incertae sedis','-1','incertae sedis','-2');
select * from val_species where "kingdom" LIKE '%ncerta%';

update val_species --make all Equisetopsida upper taxa point to incertae sedis
set "kingdom"='incertae sedis',"kingdomId"='-1',"phylum"='incertae sedis',"phylumId"='-2'
where "class" LIKE '%Equisetopsida%';

insert into val_species --create a class entry for Equisetopsida having upper taxa point at incertae sedis
("gbifId","taxonId","scientificName","acceptedNameUsage","acceptedNameUsageId","parentNameUsageId","taxonRank","taxonomicStatus","kingdom","kingdomId","phylum","phylumId")
values
(100000038,'100000038','Equisetopsida','Equisetopsida','100000038','-2','class','doubtful','incertae sedis','-1','incertae sedis','-2');
select * from val_species where "gbifId"=100000038;

