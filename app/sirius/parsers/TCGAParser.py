import os, copy
from sirius.parsers.Parser import Parser
from sirius.helpers.constants import CHROMO_IDXS, DATA_SOURCE_TCGA
import xml.etree.ElementTree as ET

class TCGA_XMLParser(Parser):
    """
    Parser for the TCGA BCR XML file format.
    One such xml file will be parsed into a dictionary that contains data for one patient
    """
    @property
    def patientdata(self):
        return self.data['patientdata']

    @patientdata.setter
    def patientdata(self, value):
        self.data['patientdata'] = value

    def parse(self):
        """
        Parse the TCGA bcr xml data format.

        Notes
        -----
        1. This method will move the openned self.filehandle to beginning of file, then read from it.
        2. Each xml file contains information for one patient.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = TCGA_XMLParser("nationwidechildrens.org_clinical.TCGA-85-6561.xml")
        >>> parser.parse()

        The parsed patient data are stored as a dictionary called self.patientdata:

        >>> print(parser.patientdata)
        {
            "additional_studies": null,
            "tumor_tissue_site": "Lung",
            "histological_type": "Lung Squamous Cell Carcinoma- Not Otherwise Specified (NOS)",
            "other_dx": "No",
            "gender": "MALE",
            "vital_status": "Alive",
            "days_to_birth": "-24234",
            "days_to_last_known_alive": null,
            "days_to_death": null,
            "days_to_last_followup": "24",
            "race_list": "\n            ",
            "bcr_patient_barcode": "TCGA-85-6561",
            "tissue_source_site": "85",
            "patient_id": "6561",
            "bcr_patient_uuid": "46f35964-bd43-47e6-83fe-40da33828c94",
            "history_of_neoadjuvant_treatment": "No",
            "informed_consent_verified": "YES",
            "icd_o_3_site": "C34.1",
            "icd_o_3_histology": "8070/3",
            "icd_10": "C34.1",
            "tissue_prospective_collection_indicator": "YES",
            "tissue_retrospective_collection_indicator": "NO",
            "days_to_initial_pathologic_diagnosis": "0",
            "age_at_initial_pathologic_diagnosis": "66",
            "year_of_initial_pathologic_diagnosis": "2011",
            "ethnicity": "NOT HISPANIC OR LATINO",
            "person_neoplasm_cancer_status": "TUMOR FREE",
            "performance_status_scale_timing": null,
            "day_of_form_completion": "23",
            "month_of_form_completion": "8",
            "year_of_form_completion": "2011",
            "stage_event": "\n            ",
            "karnofsky_performance_score": null,
            "eastern_cancer_oncology_group": null,
            "tobacco_smoking_history": "4",
            "year_of_tobacco_smoking_onset": "1975",
            "stopped_smoking_year": "2000",
            "number_pack_years_smoked": "35",
            "anatomic_neoplasm_subdivision": "R-Upper",
            "anatomic_neoplasm_subdivision_other": null,
            "diagnosis": "Lung Squamous Cell Carcinoma",
            "location_in_lung_parenchyma": "Central Lung",
            "residual_tumor": "R0",
            "kras_mutation_found": null,
            "kras_gene_analysis_performed": "NO",
            "kras_mutation_result": null,
            "egfr_mutation_performed": null,
            "egfr_mutation_result": null,
            "eml4_alk_translocation_performed": null,
            "eml4_alk_translocation_result": null,
            "eml4_alk_translocation_method": null,
            "pulmonary_function_test_performed": "NO",
            "pre_bronchodilator_fev1_percent": null,
            "post_bronchodilator_fev1_percent": null,
            "pre_bronchodilator_fev1_fvc_percent": null,
            "post_bronchodilator_fev1_fvc_percent": null,
            "dlco_predictive_percent": null,
            "radiation_therapy": null,
            "postoperative_rx_tx": null,
            "primary_therapy_outcome_success": null,
            "new_tumor_events": "\n            ",
            "drugs": "\n            ",
            "radiations": "\n            ",
            "follow_ups": "\n            "
        }
        """
        self.filehandle.seek(0)
        self.patientdata = dict()
        tree = ET.parse(self.filehandle)
        root = tree.getroot()
        patient = next(child for child in root if child.tag.endswith('patient'))
        for data in patient:
            key = data.tag.split('}')[-1]
            self.patientdata[key] = data.text

    def get_mongo_nodes(self):
        """
        Parse self.data into three types for Mongo nodes, which are the internal data structure in our MongoDB.

        Returns
        -------
        mongonodes: tuple
            The return tuple is (genome_nodes, info_nodes, edges)
            Each of the three is a list of multiple dictionaries, which contains the parsed data.

        Notes
        -----
        1. This method should be called after self.parse(), because this method will read from self.patientdata.
        2. No GenomeNodes are generated here.
        3. Each patient data is parsed as one Infonode.
        4. The InfoNode generated has _id of 'Ipatient' + TCGA patient barcode.
        5. No Edge is generated.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = GWASParser('GWAS.tsv')
        >>> parser.parse()

        Get the Mongo nodes:

        >>> genome_nodes, info_nodes, edges = parser.get_mongo_nodes()

        No genome node is generated:

        >>> print(genome_nodes)
        []

        One info node is generated for the patient:
        {
            "_id": "IpatientTCGA-85-6561",
            "type": "patient",
            "name": "Patient 6561",
            "source": "TCGA",
            "info": {
                "patient_id": "6561",
                "bcr_patient_uuid": "46f35964-bd43-47e6-83fe-40da33828c94",
                "bcr_patient_barcode": "TCGA-85-6561",
                "days_to_birth": -24234,
                "gender": "MALE",
                "tumor_tissue_site": "Lung",
                "ethnicity": "NOT HISPANIC OR LATINO",
                "diagnosis": "Lung Squamous Cell Carcinoma"
            }
        }

        No Edge is generated:

        >>> print(edges)
        []

        """
        genome_nodes, info_nodes, edges = [], [], []
        p = self.patientdata
        # create one infonode for this patient
        info_nodes = [{
            '_id': 'Ipatient' + p['bcr_patient_barcode'],
            'type': 'patient',
            'name': 'Patient ' + p['patient_id'],
            'source': DATA_SOURCE_TCGA,
            'info': {
                'patient_id': p['patient_id'],
                'bcr_patient_uuid': p['bcr_patient_uuid'].lower(),
                'bcr_patient_barcode': p['bcr_patient_barcode'],
                'days_to_birth': p['days_to_birth'],
                'gender': p['gender'],
                'tumor_tissue_site': p.get('tumor_tissue_site', 'None'),
                'ethnicity': p['ethnicity']
            }
        }]
        return genome_nodes, info_nodes, edges


class TCGA_MAFParser(Parser):
    """
    Parser for the TCGA .maf file format

    Parameters
    ----------
    filename: string
        The name of the file to be parsed.
    verbose: boolean, optional
        The flag that enables printing verbose information during parsing.
        Default is False.

    Attributes
    ----------
    filename: string
        The filename which `Parser` was initialized.
    ext: string
        The extension of the file the `Parser` was initialized.
    data: dictionary
        The internal object hold the parsed data.
    metadata: dictionary
        Points to self.data['metadata'], initilized as metadata = {'filename': filename}
    filehandle: _io.TextIOWrapper
        The filehanlde openned for self.filename.
    verbose: boolean
        The flag that enables printing verbose information during parsing.

    Methods
    -------
    parse
    get_mongo_nodes
    * inherited from parent class *
    jsondata
    save_json
    load_json
    save_mongo_nodes
    hash

    Notes
    -----
    1. The .maf file contain tab-separated values in lines.
    2. The first few lines starting with # contains metadata.
    3. The first line not starting with # contain the column labels.

    References
    ----------
    https://docs.gdc.cancer.gov/Data/File_Formats/MAF_Format/

    Examples
    --------
    Initiate a TCGA_MAFParser:

    >>> parser = TCGA_MAFParser("TCGA.maf")

    Parse the file:

    >>> parser.parse()

    Save the parsed data to a json file

    >>> parser.save_json('data.json')

    Get the Mongo nodes

    >>> mongo_nodes = parser.get_mongo_nodes()

    Save the Mongo nodes to a file

    >>> parser.save_mongo_nodes('output.mongonodes')

    """

    @property
    def mutations(self):
        return self.data['mutations']

    @mutations.setter
    def mutations(self, value):
        self.data['mutations'] = value

    def parse(self):
        """
        Parse the TCGA maf data format.

        Notes
        -----
        1. This method will move the openned self.filehandle to beginning of file, then read from it.
        2. The .maf contain tab-separated values in lines.
        3. The first few lines starting with # contains metadata.
        4. The first line not starting with # contain the column labels.

        References
        ----------
        https://docs.gdc.cancer.gov/Data/File_Formats/MAF_Format/

        Examples
        --------
        Initialize and parse the file:

        >>> parser = TCGA_MAFParser("TCGA.UCS.mutect.02747363-f04a-4ba6-a079-fe4f87853788.DR-10.0.somatic.maf.gz")
        >>> parser.parse()

        The parsed data are stored in self.data, which contains self.metadata and self.mutations:

        >>> print(parser.mutations[0])
        {
            "Hugo_Symbol": "SLC6A9",
            "Entrez_Gene_Id": "6536",
            "Center": "BI",
            "NCBI_Build": "GRCh38",
            "Chromosome": "chr1",
            "Start_Position": "44008472",
            "End_Position": "44008472",
            "Strand": "+",
            "Variant_Classification": "Silent",
            "Variant_Type": "SNP",
            "Reference_Allele": "G",
            "Tumor_Seq_Allele1": "G",
            "Tumor_Seq_Allele2": "A",
            "dbSNP_RS": "rs200658319",
            "dbSNP_Val_Status": "byCluster;byFrequency",
            "Tumor_Sample_Barcode": "TCGA-N6-A4VD-01A-11D-A28R-08",
            "Matched_Norm_Sample_Barcode": "TCGA-N6-A4VD-11A-11D-A28U-08",
            "Match_Norm_Seq_Allele1": "",
            "Match_Norm_Seq_Allele2": "",
            "Tumor_Validation_Allele1": "G",
            "Tumor_Validation_Allele2": "A",
            "Match_Norm_Validation_Allele1": "",
            "Match_Norm_Validation_Allele2": "",
            "Verification_Status": "",
            "Validation_Status": "",
            "Mutation_Status": "Somatic",
            "Sequencing_Phase": "",
            "Sequence_Source": "",
            "Validation_Method": "RNA",
            "Score": "",
            "BAM_File": "",
            "Sequencer": "Illumina HiSeq 2000",
            "Tumor_Sample_UUID": "2d33a481-d2dd-4205-86a8-bf35b0de3a71",
            "Matched_Norm_Sample_UUID": "cda4ae33-d178-4d5d-8f91-aa585bcf4a79",
            "HGVSc": "c.690C>T",
            "HGVSp": "p.=",
            "HGVSp_Short": "p.A230A",
            "Transcript_ID": "ENST00000360584",
            "Exon_Number": "5/14",
            "t_depth": "103",
            "t_ref_count": "65",
            "t_alt_count": "38",
            "n_depth": "132",
            "n_ref_count": "",
            "n_alt_count": "",
            "all_effects": "SLC6A9,synonymous_variant,p.A157A,ENST00000372310,NM_001024845.2,c.471C>T,LOW,,,,-1;SLC6A9,synonymous_variant,p.A230A,ENST00000360584,NM_201649.3,c.690C>T,LOW,YES,,,-1;SLC6A9,synonymous_variant,p.A176A,ENST00000357730,NM_001261380.1&NM_006934.3,c.528C>T,LOW,,,,-1;SLC6A9,synonymous_variant,p.A92A,ENST00000372307,,c.276C>T,LOW,,,,-1;SLC6A9,synonymous_variant,p.A157A,ENST00000372306,,c.471C>T,LOW,,,,-1;SLC6A9,synonymous_variant,p.A46A,ENST00000475075,,c.138C>T,LOW,,,,-1;SLC6A9,downstream_gene_variant,,ENST00000466926,,,MODIFIER,,,,-1;SLC6A9,downstream_gene_variant,,ENST00000528803,,,MODIFIER,,,,-1;SLC6A9,downstream_gene_variant,,ENST00000492434,,,MODIFIER,,,,-1;SLC6A9,downstream_gene_variant,,ENST00000489764,,,MODIFIER,,,,-1",
            "Allele": "A",
            "Gene": "ENSG00000196517",
            "Feature": "ENST00000360584",
            "Feature_type": "Transcript",
            "One_Consequence": "synonymous_variant",
            "Consequence": "synonymous_variant",
            "cDNA_position": "882/2330",
            "CDS_position": "690/2121",
            "Protein_position": "230/706",
            "Amino_acids": "A",
            "Codons": "gcC/gcT",
            "Existing_variation": "rs200658319",
            "ALLELE_NUM": "1",
            "DISTANCE": "",
            "TRANSCRIPT_STRAND": "-1",
            "SYMBOL": "SLC6A9",
            "SYMBOL_SOURCE": "HGNC",
            "HGNC_ID": "HGNC:11056",
            "BIOTYPE": "protein_coding",
            "CANONICAL": "YES",
            "CCDS": "CCDS41317.1",
            "ENSP": "ENSP00000353791",
            "SWISSPROT": "P48067",
            "TREMBL": "",
            "UNIPARC": "UPI000053030B",
            "RefSeq": "NM_201649.3",
            "SIFT": "",
            "PolyPhen": "",
            "EXON": "5/14",
            "INTRON": "",
            "DOMAINS": "Pfam_domain:PF00209;PROSITE_profiles:PS50267",
            "GMAF": "",
            "AFR_MAF": "",
            "AMR_MAF": "",
            "ASN_MAF": "",
            "EAS_MAF": "",
            "EUR_MAF": "",
            "SAS_MAF": "",
            "AA_MAF": "0.0002",
            "EA_MAF": "0",
            "CLIN_SIG": "",
            "SOMATIC": "",
            "PUBMED": "",
            "MOTIF_NAME": "",
            "MOTIF_POS": "",
            "HIGH_INF_POS": "",
            "MOTIF_SCORE_CHANGE": "",
            "IMPACT": "LOW",
            "PICK": "1",
            "VARIANT_CLASS": "SNV",
            "TSL": "1",
            "HGVS_OFFSET": "",
            "PHENO": "",
            "MINIMISED": "1",
            "ExAC_AF": "5.766e-05",
            "ExAC_AF_Adj": "5.778e-05",
            "ExAC_AF_AFR": "9.626e-05",
            "ExAC_AF_AMR": "0",
            "ExAC_AF_EAS": "0",
            "ExAC_AF_FIN": "0",
            "ExAC_AF_NFE": "0",
            "ExAC_AF_OTH": "0",
            "ExAC_AF_SAS": "0.0003634",
            "GENE_PHENO": "",
            "FILTER": "PASS",
            "CONTEXT": "ACACCGGCGCA",
            "src_vcf_id": "85b191fa-e8f6-4d97-bc13-0bc236cee6c8",
            "tumor_bam_uuid": "3d52eda9-5f36-4a55-8a3c-07a576a0ef1f",
            "normal_bam_uuid": "6c3037f4-9838-4b36-9551-99e91cb85ef0",
            "case_id": "14213209-2217-4812-9a19-d9b2b6718467",
            "GDC_FILTER": "",
            "COSMIC": "COSM6063461;COSM6063462",
            "MC3_Overlap": "True",
            "GDC_Validation_Status": "Valid"
        }

        """
        # start from the beginning for reading
        self.filehandle.seek(0)
        self.mutations = []
        # read the first line as labels
        labels = None
        # read the rest of the lines as data
        for line in self.filehandle:
            line = line.strip()
            if line[0] == '#':
                key, value = line[1:].split(maxsplit=1)
                self.metadata[key] = value
            elif line:
                if labels == None:
                    labels = line.split('\t')
                else:
                    ls = line.split('\t')
                    self.mutations.append(dict(zip(labels, ls)))
                    if self.verbose and len(self.mutations) % 100000 == 0:
                        print("%d data parsed" % len(self.mutations), end='\r')

    def get_mongo_nodes(self, patient_barcode_tumor_site=None):
        """
        Parse self.data into three types for Mongo nodes, which are the internal data structure in our MongoDB.

        Returns
        -------
        mongonodes: tuple
            The return tuple is (genome_nodes, info_nodes, edges)
            Each of the three is a list of multiple dictionaries, which contains the parsed data.

        Notes
        -----
        1. This method should be called after self.parse(), because this method will read from self.metadata and self.mutations, which are contents of self.data.
        2. GenomeNodes generated are mostly SNPs. The ones with a known rs number will have ID `Gsnp_rs****`, others will have normalized ID `Gv_(hash of contig_pos_ref_alt)`.
        3. Data with the same variant ID will be aggregated into one genome node. It will be further aggregated into the database when uploading.
        4. No Infonode is generated.
        5. No Edge is generated.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = TCGA_MAFParser("TCGA.UCS.mutect.02747363-f04a-4ba6-a079-fe4f87853788.DR-10.0.somatic.maf.gz")
        >>> parser.parse()

        Get the Mongo nodes:

        >>> genome_nodes, info_nodes, edges = parser.get_mongo_nodes()

        GenomeNodes generated here are SNPs. The type will be `variant` if no RS number is found

        >>> print(genome_nodes[0])
        {
            "_id": "Gsnp_rs200658319",
            "contig": "chr1",
            "type": "SNP",
            "start": 44008472,
            "end": 44008472,
            "length": 1,
            "source": "TCGA",
            "name": "SNP of SLC6A9",
            "info": {
            "genes": [
                "SLC6A9"
            ],
            "strand": "+",
            "score": "",
            "filter": "PASS",
            "variant_ref": "G",
            "variant_alt": "A",
            "variant_tags": [
                "synonymous_variant"
            ],
            "variant_affected_feature_types": [
                "Transcript"
            ],
            "variant_affected_bio_types": [
                "protein_coding"
            ],
            "Tumor_Sample_Barcodes": [
                "TCGA-N6-A4VD-01A-11D-A28R-08"
            ],
            "Mutation_Status": "Somatic",
            "Transcript_IDs": [
                "ENST00000360584"
            ],
            "CCDS": "CCDS41317.1",
            "ENSP": "ENSP00000353791"
            }
        }


        No infonodes are generated

        >>> print(info_nodes)
        []

        No edges are generated:

        >>> print(edges)
        []

        """
        genome_nodes, info_nodes, edges = [], [], []
        gid_idx_dict = dict()
        for d in copy.deepcopy(self.mutations):
            # create GenomeNode for variants
            contig = d['Chromosome']
            ref, alt = d['Reference_Allele'], d['Allele']
            rs_number = d['dbSNP_RS']
            if rs_number[:2] == 'rs':
                gid = 'Gsnp_' + rs_number
                gtype = 'SNP'
                # we skip adding gnode here since it's alreday in dbSNP.
            else:
                variant_key_string = '_'.join([contig, d['Start_Position'], ref, alt])
                gid = 'Gv_' + self.hash(variant_key_string)
                gtype = 'variant'
            gene_name = d['Hugo_Symbol']
            if gid in gid_idx_dict:
                # aggregate results into existing gnode
                gnode = genome_nodes[gid_idx_dict[gid]]
                gnode['info']['genes'].append(gene_name)
                gnode['info']['variant_tags'] += d['Consequence'].split(';')
                gnode['info']['variant_affected_feature_types'].append(d['Feature_type'])
                gnode['info']['variant_affected_bio_types'].append(d['BIOTYPE'])
                gnode['info']['Tumor_Sample_Barcodes'].append(d['Tumor_Sample_Barcode'])
                gnode['info']['Transcript_IDs'].append(d['Transcript_ID'])
            else:
                gid_idx_dict[gid] = len(genome_nodes)
                name = d['Variant_Type'] + ' of ' + gene_name
                pos = int(d['Start_Position'])
                gnode = {
                    "_id": gid,
                    "contig": contig,
                    "type": gtype,
                    "start": pos,
                    "end": pos,
                    "length": 1,
                    "source": DATA_SOURCE_TCGA,
                    "name": name,
                    "info": {
                        'genes': [gene_name],
                        'strand': d['Strand'],
                        'score': d['Score'],
                        'filter': d['FILTER'],
                        'variant_ref': ref,
                        'variant_alt': alt,
                        'variant_tags': d['Consequence'].split(';'),
                        'variant_affected_feature_types': [d['Feature_type']],
                        'variant_affected_bio_types': [d['BIOTYPE']],
                        'Tumor_Sample_Barcodes': [d['Tumor_Sample_Barcode']],
                        'Mutation_Status': d['Mutation_Status'],
                        'Transcript_IDs': [d['Transcript_ID']],
                        'CCDS': d['CCDS'],
                        'ENSP': d['ENSP'],
                    }
                }
                genome_nodes.append(gnode)
                if self.verbose and len(genome_nodes) % 100000 == 0:
                    print(f"{len} genome_nodes parsed \r")
        # final processing for all the genome_nodes generated
        for gnode in genome_nodes:
            # make sure arrays have unique values
            for k in ('genes', 'variant_tags', 'variant_affected_feature_types', 'variant_affected_bio_types',
                      'Tumor_Sample_Barcodes', 'Transcript_IDs'):
                if len(gnode['info'][k]) > 1:
                    gnode['info'][k] = list(set(gnode['info'][k]))
        # get tumor site if information available
        if patient_barcode_tumor_site != None:
            for gnode in genome_nodes:
                tumor_sites = set()
                for tumor_barcode in gnode['info']['Tumor_Sample_Barcodes']:
                    patient_barcode = '-'.join(tumor_barcode.split('-',3)[:3])
                    tumor_sites.add(patient_barcode_tumor_site.get(patient_barcode, 'N/A'))
                gnode['info']['tumor_tissue_sites'] = list(tumor_sites)
        return genome_nodes, info_nodes, edges

class TCGA_CNVParser(Parser):
    """
    Parser for the TCGA Copy Number Variation (CNV) data format

    Parameters
    ----------
    filename: string
        The name of the file to be parsed.
    verbose: boolean, optional
        The flag that enables printing verbose information during parsing.
        Default is False.

    Attributes
    ----------
    filename: string
        The filename which `Parser` was initialized.
    ext: string
        The extension of the file the `Parser` was initialized.
    data: dictionary
        The internal object hold the parsed data.
    metadata: dictionary
        Points to self.data['metadata'], initilized as metadata = {'filename': filename}
    filehandle: _io.TextIOWrapper
        The filehanlde openned for self.filename.
    verbose: boolean
        The flag that enables printing verbose information during parsing.

    Methods
    -------
    parse
    get_mongo_nodes
    * inherited from parent class *
    jsondata
    save_json
    load_json
    save_mongo_nodes
    hash

    Notes
    -----
    1. This method will move the openned self.filehandle to beginning of file, then read from it.
    2. The .seg.v2.txt file contain tab-separated values in lines.
    3. The first line contains labels.

    References
    ----------
    https://docs.gdc.cancer.gov/Data/Bioinformatics_Pipelines/CNV_Pipeline/

    Examples
    --------
    Initiate a TCGA_CNVParser:

    >>> parser = TCGA_CNVParser("CLADE_p_TCGASNP_184_195_N_GenomeWideSNP_6_G03_1039872.nocnv_grch38.seg.v2.txt")

    Parse the file:

    >>> parser.parse()

    Save the parsed data to a json file

    >>> parser.save_json('data.json')

    Get the Mongo nodes, pass the tumor_tissue_site string as an input argument

    >>> mongo_nodes = parser.get_mongo_nodes("Lung")

    Save the Mongo nodes to a file

    >>> parser.save_mongo_nodes('output.mongonodes')

    """

    @property
    def cnvs(self):
        return self.data['cnvs']

    @cnvs.setter
    def cnvs(self, value):
        self.data['cnvs'] = value

    def parse(self):
        """
        Parse the TCGA Copy Number Variation (CNV) data format.

        Notes
        -----
        1. This method will move the openned self.filehandle to beginning of file, then read from it.
        2. The .seg.v2.txt file contain tab-separated values in lines.
        3. The first line contains labels.

        References
        ----------
        https://docs.gdc.cancer.gov/Data/Bioinformatics_Pipelines/CNV_Pipeline/

        Examples
        --------
        Initialize and parse the file:

        >>> parser = TCGA_CNVParser("CLADE_p_TCGASNP_184_195_N_GenomeWideSNP_6_G03_1039872.nocnv_grch38.seg.v2.txt")
        >>> parser.parse()

        The parsed data are stored in self.data, which contains self.metadata and self.mutations:

        >>> print(parser.cnvs[0])
        {
            "GDC_Aliquot": "370a1c0f-78d2-4330-a6fb-f644064b8ed7",
            "Chromosome": "1",
            "Start": "3301765",
            "End": "23733863",
            "Num_Probes": "11494",
            "Segment_Mean": "0.0219"
        }

        """
        # the first line contains lables
        self.filehandle.seek(0)
        title_line = self.filehandle.readline().strip()
        labels = title_line.split('\t')
        self.cnvs = []
        for line in self.filehandle:
            ls = line.strip().split('\t')
            cnv = dict(zip(labels, ls))
            self.cnvs.append(cnv)

    def get_mongo_nodes(self, tumor_tissue_site='Unknown'):
        """
        Parse self.data into three types for Mongo nodes, which are the internal data structure in our MongoDB.

        Returns
        -------
        mongonodes: tuple
            The return tuple is (genome_nodes, info_nodes, edges)
            Each of the three is a list of multiple dictionaries, which contains the parsed data.

        Notes
        -----
        1. This method should be called after self.parse(), because this method will read from self.metadata and self.cnvs, which are contents of self.data.
        2. GenomeNodes generated are of type 'copy_number_variation'.
        3. The tumor_tissue_site is passed as a parameter to add as `info.tumor_tissue_site` for each genome node.
        4. No Infonode is generated.
        5. No Edge is generated.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = TCGA_CNVParser("CLADE_p_TCGASNP_184_195_N_GenomeWideSNP_6_G03_1039872.nocnv_grch38.seg.v2.txt")
        >>> parser.parse()

        Get the Mongo nodes:

        >>> genome_nodes, info_nodes, edges = parser.get_mongo_nodes("Lung")

        GenomeNodes generated here are of type "copy_number_variation"

        >>> print(genome_nodes[0])
        {
            "_id": "Gcnv_238c2bf323f7c5819e4493c2de05d5ddfee2048989ddeab8d2a464829054f967",
            "type": "copy_number_variation",
            "contig": "chr1",
            "start": 3301765,
            "end": 23733863,
            "source": "TCGA",
            "length": 20432099,
            "name": "CNV",
            "info": {
                "cnv_n_probes": 11494,
                "cnv_seg_mean": 0.0219,
                "GDC_Aliquot": "370a1c0f-78d2-4330-a6fb-f644064b8ed7",
                "tumor_tissue_site": "Lung"
            }
        }

        No infonodes are generated

        >>> print(info_nodes)
        []

        No edges are generated:

        >>> print(edges)
        []

        """
        genome_nodes, info_nodes, edges = [], [], []
        for d in self.cnvs.copy():
            contig = 'chr' + d['Chromosome']
            start = int(d['Start'])
            end = int(d['End'])
            num_probes = int(d['Num_Probes'])
            segment_mean = float(d['Segment_Mean'])
            gid = 'Gcnv_' + self.hash(d['GDC_Aliquot'] + contig + d['Start'] + d['End'])
            gnode = {
                '_id': gid,
                'type': 'copy_number_variation',
                'contig': contig,
                'start': start,
                'end': end,
                'source': DATA_SOURCE_TCGA,
                'length': end-start+1,
                'name': 'CNV',
                'info': {
                    'cnv_n_probes': num_probes,
                    'cnv_seg_mean': segment_mean,
                    'GDC_Aliquot': d['GDC_Aliquot'],
                    'tumor_tissue_site': tumor_tissue_site
                }
            }
            genome_nodes.append(gnode)
        return genome_nodes, info_nodes, edges
