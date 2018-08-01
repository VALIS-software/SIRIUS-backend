from sirius.mongo import GenomeNodes
from sirius.core.utilities import threadsafe_lru

@threadsafe_lru(maxsize=1024)
def get_reference_gene_data(contig):
    """ Find all genes in a contig """
    # First we find all the genes
    gene_types = ['gene', 'pseudogene']
    gene_projection = ['_id', 'contig', 'start', 'length', 'name', 'info.strand']
    all_genes = sorted(GenomeNodes.find({'contig': contig, 'type': {'$in': gene_types}}, projection=gene_projection), key=lambda x: x['start'])
    # second we convert `_id` to `id`
    for gene in all_genes:
        gene['id'] = gene.pop('_id')
        gene['strand'] = gene.pop('info').pop('strand')
    return all_genes

@threadsafe_lru(maxsize=1024)
def get_reference_hierarchy_data(contig):
    """ Find all genes in a contig, then build the gene->transcript->exon hierarchy """
    # First we find all the genes
    gene_types = ['gene', 'pseudogene']
    gene_projection = ['_id', 'contig', 'start', 'length', 'name', 'info.strand']
    all_genes = sorted(GenomeNodes.find({'contig': contig, 'type': {'$in': gene_types}}, projection=gene_projection), key=lambda x: x['start'])
    # Second store their index
    gene_idx_dict = dict()
    for i, gene in enumerate(all_genes):
        gene['id'] = gid = gene.pop('_id')
        gene['strand'] = gene.pop('info').pop('strand')
        gene['transcripts'] = []
        gene_idx_dict[gid] = i
    # Third we find all the transcripts
    transcript_types = ['transcript', 'pseudogenic_transcript', 'miRNA', 'lnc_RNA', 'mRNA']
    transcript_projection = ['_id', 'contig', 'start', 'length', 'name', 'info.strand', 'info.Parent']
    all_transcripts = sorted(GenomeNodes.find({'contig': contig, 'type': {'$in': transcript_types}}, projection=transcript_projection), key=lambda x: x['start'])
    # Fourth we put the transcripts into genes and store their parent genes
    gene_transcript_idx_dict = dict()
    for transcript in all_transcripts:
        parent = transcript['info'].pop('Parent', None)
        if parent != None:
            parent_id = 'G' + parent.split(':')[-1]
            gene_idx = gene_idx_dict.get(parent_id, None)
            if gene_idx != None:
                transcript['id'] = gid = transcript.pop('_id')
                transcript['strand'] = transcript.pop('info').pop('strand')
                transcript['components'] = []
                gene_transcript_idx_dict[gid] = (gene_idx, len(all_genes[gene_idx]['transcripts']))
                all_genes[gene_idx]['transcripts'].append(transcript)
    # Fifth we find all the exons
    exon_projection = ['_id', 'contig', 'start', 'length', 'name', 'info.strand', 'info.Parent']
    all_exons = sorted(GenomeNodes.find({'contig': contig, 'type': 'exon'}, projection=exon_projection), key=lambda x: x['start'])
    # Sixth we put all the exons into their parent transcripts
    for exon in all_exons:
        parent = exon['info'].pop('Parent', None)
        if parent != None:
            parent_id = 'G' + parent.split(':')[-1]
            gene_idx, transcript_idx = gene_transcript_idx_dict.get(parent_id, (None, None))
            if gene_idx != None:
                exon['id'] = exon.pop('_id')
                exon['strand'] = exon.pop('info').pop('strand')
                all_genes[gene_idx]['transcripts'][transcript_idx]['components'].append(exon)
    return all_genes
