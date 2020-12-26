--count iNat vernacular names added over the week of 12/7/20
select count(*) from val_vernacular where source like 'iNat%'; --6333

--count iNat vernacular names added over the week of 12/7/20
select count(*) from val_vernacular where source = 'Species List for the State of Vermont'; --117 on 12/10/20

--count taxa in val_species lacking vernacularName in val_vernacular
select s."taxonId" from val_species s
left join val_vernacular v on v."taxonId"=s."taxonId"
where v."taxonId" is null; --11211 on 12/9/20, 11188 after adding VT Registry names from val_species (below)

--total unique taxa
select count("taxonId") as taxon_count from val_species;
--count distinct taxa in val_species having >0 vernacularName in val_vernacular
select count(distinct(v."taxonId")) as join_count from val_species s
inner join val_vernacular v on v."taxonId"=s."taxonId"; --12101 on 12/10/20
--aka
select count(distinct("taxonId")) as vernacular_count from val_vernacular; --12101 on 12/10/20

--create table val_vernacular_after_inat as select * from val_vernacular;

--val_species vernacularNames
select "taxonId", "scientificName", "vernacularName" from val_species s where "vernacularName" <> ''; --919 on 12/9/20

--val_species vernacularNames already in val_vernacular
select s."taxonId", s."scientificName", s."vernacularName" from val_species s
inner join val_vernacular v on v."vernacularName"=s."vernacularName"; --753 on 12/9/20

--val_species vernacularNames not in val_vernacular
select s."taxonId", s."scientificName", s."vernacularName" from val_species s
left join val_vernacular v on s."vernacularName"=v."vernacularName"
where s."vernacularName" <> '' and v."vernacularName" is null;

--select missing vernacularNames from val_species into val_vernacular
--NOTE that 
INSERT INTO val_vernacular ("taxonId","scientificName","vernacularName","source","preferred")
SELECT s."taxonId",s."scientificName",s."vernacularName",'Species List for the State of Vermont' as source,'t' as preferred
FROM val_species s
LEFT JOIN val_vernacular v ON LOWER(s."vernacularName")=LOWER(v."vernacularName")
WHERE s."vernacularName" <> '' AND v."vernacularName" is null;
