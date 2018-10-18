# parent Parser class
from sirius.parsers.parser import Parser
# child classes
from sirius.parsers.bed_parser import BEDParser, BEDParser_ENCODE, BEDParser_ROADMAP_EPIGENOMICS
from sirius.parsers.eqtl_parser import EQTLParser, EQTLParser_exSNP, EQTLParser_GTEx
from sirius.parsers.fasta_parser import FASTAParser
from sirius.parsers.gff_parser import GFFParser, GFFParser_ENSEMBL, GFFParser_RefSeq
from sirius.parsers.obo_parser import OBOParser, OBOParser_EFO
from sirius.parsers.tcga_parser import TCGA_CNVParser, TCGA_MAFParser, TCGA_XMLParser
from sirius.parsers.tsv_parser import TSVParser, TSVParser_ENCODEbigwig, TSVParser_GWAS, TSVParser_HGNC
from sirius.parsers.txt_parser import TxtParser, TxtParser_23andme
from sirius.parsers.vcf_parser import VCFParser, VCFParser_ClinVar, VCFParser_dbSNP, VCFParser_ExAC
from sirius.parsers.kegg_parser import KEGG_XMLParser
from sirius.parsers.special_parser import Parser_NatureCasualVariants