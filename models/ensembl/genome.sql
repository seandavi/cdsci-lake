-- description: One row per (genome assembly, taxon, Ensembl release) landed in lake.ensembl.gtf.
-- license: ensembl-no-restrictions
-- column genome_id: The assembly's INSDC accession (e.g. GCA_000001405.29) as Ensembl's species_EnsemblVertebrates.txt reports it for the release — a citable external identifier, not a synthesized key.
-- column assembly_name: Ensembl's assembly build name (GRCh38.p14, R64-1-1) for the same release.
-- ensembl.genome (ADR-0015): the assembly facts ride on every raw GTF row
-- because they are not *in* the GTF body — Ensembl publishes them in the
-- per-release species_EnsemblVertebrates.txt, which the EL reads and stamps at
-- land time. So this model is a DISTINCT over the stamps, not a parse.
SELECT DISTINCT
    genome_accession AS genome_id,
    ncbitaxon_id,
    ensembl_release,
    species,
    assembly AS assembly_name,
    'ENSEMBL' AS source
FROM lake.ensembl.gtf
