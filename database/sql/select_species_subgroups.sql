/*
Copy (
SELECT json_build_object(
	SELECT json_agg(taxon) FROM (
		select "class", "scientificName" as order, "vernacularName" as common from val_species where "taxonRank"='order'
		and class in (select distinct("scientificName") as class from val_species where "taxonRank"='class' )
		) as taxon
	) 
)
to 'C:\Users\jloomis\Documents\VCE\VAL_Temp\val_subgroups_class.json' with NULL '';
*/

Copy (
select json_agg(
	select "class", "scientificName" as order, "vernacularName" as common from val_species where "taxonRank"='order'
	and class in (select distinct("scientificName") as class from val_species where "taxonRank"='class' )
	)
) 
to 'C:\Users\jloomis\Documents\VCE\VAL_Temp\val_subgroups_class.json' with NULL '';

















/*
select distinct("scientificName") from val_species where "taxonRank"='kingdom'; --11
select distinct("scientificName") from val_species where "taxonRank"='phylum'; --68
select distinct("scientificName") from val_species where "taxonRank"='class'; --159
select distinct("scientificName") from val_species where "taxonRank"='order'; --471
select distinct("scientificName") from val_species where "taxonRank"='family'; --1621

select * from val_species where "scientificName" like '%Insect%';

select distinct("scientificName") from val_species where "taxonRank"='order' and class='Insecta'; --19
select distinct("scientificName") from val_species where "taxonRank"='family' and class='Insecta'; --383
select distinct("scientificName") from val_species where "taxonRank"='species' and class='Insecta'; --5782

select distinct("scientificName") from val_species where "taxonRank"='species' and class='Insecta'; --5782

select * from val_species where "scientificName" like '%Hemiptera%';
*/