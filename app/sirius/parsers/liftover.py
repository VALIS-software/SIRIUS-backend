import pyliftover
import os

this_file_folder = os.path.dirname(os.path.realpath(__file__))
lo = pyliftover.LiftOver(os.path.join(this_file_folder, 'hg19ToHg38.over.chain.gz'))