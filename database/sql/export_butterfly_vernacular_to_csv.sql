--NOTE: pg just needs permissions to write to the specified folder. chmod 777 \dir\subdir.
copy (
select v."taxonId",v."scientificName",s."taxonRank",v."vernacularName",v."preferred",v."source" from val_vernacular v
inner join val_species s on v."taxonId"=s."taxonId"
where s."familyId" IN ('6953', '5437', '7017', '9417', '5481', '1933999')
or s."family" IN ('Hesperiidae','Lycaenidae','Nymphalidae','Papilionidae','Pieridae','Riodinidae')
--taxon_key=6953&taxo	n_key=5473&taxon_key=7017&taxon_key=9417&taxon_key=5481&taxon_key=1933999
--6953=Hesperiidae
--5473=Lycaenidae
--7017=Nymphalidae
--9417=Papilionidae
--5481=Pieridae
--1933999=Riodinidae
)
TO 'C:\Users\jtloo\Documents\VCE\VAL_Data_Pipelines\VAL_NameIndex\repo\database\export\butterfly_vernacular.csv' delimiter ',' csv header;