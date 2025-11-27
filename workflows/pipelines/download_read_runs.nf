#!/usr/bin/env nextflow

params.sample = "SAMN10879440"
params.publishDir = null

process downloadFastq {
    publishDir params.publishDir ?: "results", mode: 'copy', enabled: params.publishDir != null

    input:
        tuple val(run_accession), val(fastq_ftp)
    output:
        path "${run_accession}_*.fastq"
    script:
        """
        IFS=";"
        ITER=1
        read -ra urls <<< "$fastq_ftp"
        for url in "\${urls[@]}"; do
            echo \$url
            curl -sS http://\$url -o ${run_accession}_\$ITER.fastq.gz
            gunzip ${run_accession}_\$ITER.fastq.gz
            ((++ITER))
        done
        """
}

workflow {
    filereport = Channel.fromPath(
        "https://www.ebi.ac.uk/ena/portal/api/filereport?accession=${params.sample}&result=read_run&fields=run_accession,fastq_ftp",
        glob:false
    ) \
    | splitCsv(sep:'\t', header:true) \
    | map { row -> [row.run_accession, row.fastq_ftp] } \
    | downloadFastq
    | countFastq
    | sum()
    | view()
}
