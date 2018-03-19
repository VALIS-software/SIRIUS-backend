# SIRIUS-backend Spec Sheet
Yudong Qiu
Feb. 25th 2018

Notes:
* This spec sheet may subject to major change based on the progress of development
* All Flask API implementation can be found in views.py

-----------------------------------
STATIC URLs: (Handled by Nginx)
-----------------------------------

`/`  -->  valis-dist/index.html

`/index`  -->  valis-dist/index.html

`/*`      --> valis-dist/static/*   (for any URL not implemented below)



-----------------------------------
Implemented URLs                
-----------------------------------

I. ANNOTATION TRACK
----------------------------------
real Annotations: `GRCh38`
mock Annotations: `GRCh38_genes, cross-track-test-1, cross-track-test-2`

`/annotations`  -->  list of annotation ids, including both real and mock annotations

    Example: /annotations

    Output: ["GRCh38_genes", "cross-track-test-1", "cross-track-test-2", "GWASCatalog", "GRCh38"]

`/annotations/<string:annotation_id>`  -->  dictionary contains information of the annotation

    Example: /annotations/GRCh38

      Output: {"annotationId": "GRCh38", "startBp": 1, "endBp": 3088269832}

`/annotations/<string:annotation_ids>/<int:start_bp>/<int:end_bp>`    -->  dictionary of data to show on the annotation track

    Optional keywords: sampling_rate, track_height_px

    Example: /annotations/GRCh38/88080384/100663296?sampling_rate=12288&track_height_px=72

      Output: {"startBp": 88080384, "endBp": 100663296, "samplingRate": 12288, "trackHeightPx": 72, "annotationIds": "GRCh38", "values": [value1, value2..]}

      value1 = {"id": "gene2044", "labels": [["EVI5", true, 0, 0, 0]], "yOffsetPx": 0, "heightPx": 22, "segments": [[0, 283709, null, [0.13, 0.19, 0.14, 1.0], 20]], "entity": entity, "startBp": 92508696, "endBp": 92792404}}

      entity = {"_id": "geneid_7813", "assembly": "GRCh38", "sourceurl": "https://www.ncbi.nlm.nih.gov/projects/genome/guide/human/index.shtml", "type": "gene", "location": "Chr1", "info": {"seqid": "NC_000001.11", "source": "BestRefSeq%2CXM/XP/XR", "score": ".", "strand": "-", "phase": ".", "attributes": {"ID": "gene2044", "Name": "EVI5", "description": "ecotropic viral integration site 5", "gbkey": "Gene", "gene": "EVI5", "gene_biotype": "protein_coding", "gene_synonym": "EVI-5,NB4S", "GeneID": "7813", "HGNC": "HGNC:3501", "MIM": "602942"}}, "start": 92508696, "end": 92792404, "length": 283709}

`[POST] /annotations/<string:annotation_ids>/<int:start_bp>/<int:end_bp>`    -->  dictionary of data to show query results

    POST: query dictionary (see query.md for more details)

    Optional keywords: sampling_rate, track_height_px

    (Aggregated Results)

    Example: [POST] /annotations/GWASCatalog/704643072/939524096?sampling_rate=229376&track_height_px=72

      Output: {"startBp": 704643072, "endBp": 939524096, "samplingRate": 229376, "trackHeightPx": 72, "annotationIds": "GWASCatalog", "values": [value1, value2,...]

      value1 = {"id": "cluster", "startBp": 706832046, "endBp": 706832046, "labels": [["1", true, 4, 0, 0]], "yOffsetPx": 0, "heightPx": 72, "segments": [[0, 1, null, [0.15, 0.55, 1.0, 0.2], 20]], "entity": [entity1, ..]  }

      entity = {"_id": "snp_rs77485526", "type": "SNP", "location": "Chr4", "start": 17386536, "end": 17386536, "length": 1, "sourceurl": "www.ebi.ac.uk/gwas", "assembly": "GRCh38", "info": {"ID": "rs77485526", "Name": "rs77485526", "mapped_gene": "LOC107986219 - RPS7P6"}}



II. DATA TRACK
--------------------------------
mock Data: sequence, GM12878-DNase, K562-DNase, MCF7-DNase

`/tracks`   -->  list of all track_ids

    Example: /tracks

      Output: ["GM12878-DNase", "K562-DNase", "MCF7-DNase", "sequence"]

`/tracks/<string:track_id>`  -->  dictionary contains information of track

    Example: /tracks/sequence

      Output: {"trackId": "sequence", "startBp": 0, "endBp": 3000000000, "dataType": "sequence"}

`/tracks/<string:track_id>/<int:start_bp>/<int:end_bp>`  -->  dictionary contains data in track

    Example: /tracks/sequence/0/3

      Output: {"startBp": 0, "endBp": 3, "samplingRate": 1, "numSamples": 3, "trackHeightPx": 0, "values": [0.0, 0, 0.0, 0.5, 0, 4.0e-09, 0.75, 0, 8.0e-09], "dimensions": ["symbol", "chromsome_index", "chromosome_location"], "dataType": "basepairs"}



IV. GRAPHS
-----------------------------
`/graphs`  -->  all available graph IDs

    Example: /graphs

      Output: ["ld_score"]

`/graphs/<string:graph_id>/<string:annotation_id1>/<string:annotation_id2>/<int:start_bp>/<int:end_bp>`  -->  Graph data linking two annotation tracks.

    Optional Keywords: sampling_rate, base_pair_offset

    Example: /graphs/ld_score/cross-track-test-1/cross-track-test-2/0/10000

      Output: {"startBp": 0, "endBp": 10000, "samplingRate": 1, "graphId": "ld_score", "annotationIds": ["cross-track-test-1", "cross-track-test-2"], "values": [[763713556, 156874026, 0.20263008681257]]}


V. OTHERS
----------------------------
`/track_info` --> information for all imported datasets in the database.

    Example: `/track_info`

      Output:

      ```
      [
        {
          "track_type": "sequence",
          "title": "Sequence Tracks",
          "description": "Raw sequence data"
        },
        {
          "track_type":
          "gwas",
          "title": "Genome Wide Associations",
          "description": "Variants related to traits or diseases."
        },
        {
          "track_type": "eqtl",
          "title": "Quantitative Trait Loci",
          "description": "Variants related to change in gene expression."
        },
        {
          "track_type": "GRCh38_gff",
          "title": "Genome Elements",
          "description": "Genes, Promoters, Enhansers, TF Sites, etc."
        }
      ]
      ```

    These information are used in the DatasetSelector side bar of the front end.

`/distinct_values/<string:query_type>/<string:index>` --> All distinct values of a certain Node's certain index

    Example: `/distinct_values/GenomeNode/type`

      Output:

      ```
      ["CDS", "C_gene_segment", "D_gene_segment", "J_gene_segment", "RNase_MRP_RNA", "RNase_P_RNA", "SNP", "SRP_RNA", "V_gene_segment", "Y_RNA", "antisense_RNA", "cDNA_match", "centromere", "enhancer", "exon", "gene", "lnc_RNA", "mRNA", "match", "miRNA", "ncRNA", "primary_transcript", "promoter", "rRNA", "region", "repeat_region", "snRNA", "snoRNA", "tRNA", "telomerase_RNA", "transcript", "vault_RNA"]
      ```

    This API is useful to provide autocomplete or select field information in the front end. Only the following indices are allowed to prevent any misuse of this end point to cause server crash.

    `GenomeNode`: 'type', 'chromid', 'assembly', 'sourceurl'

    `InfoNode`: 'type', 'name', 'sourceurl'

    `EdgeNode`: 'type', 'from_type', 'to_type', 'sourceurl'
