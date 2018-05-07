class XAxis(object):
    """
    The XAxis object is helpful for converting relative and absolute coordinates

    Parameters
    ----------
    assembly_info: dict
        The information of assembly needed to set up the axis

    Methods
    -------
    abs_to_rel
    rel_to_abs

    """
    def __init__(self, assembly_info):
        # set up some chromo lengths information for coordinate convertion
        self.track_start_bp = assembly_info['start_bp']
        self.track_end_bp = assembly_info['end_bp']
        self.chromo_lengths = assembly_info['chromo_lengths']
        self.chromo_end_bps = [self.track_start_bp - 1]
        for l in self.chromo_lengths:
            self.chromo_end_bps.append(self.chromo_end_bps[-1] + l)
        assert self.track_end_bp == self.chromo_end_bps[-1], 'Sum of chromo_lengths should be consistent with start_bp and end_bp'

    def abs_to_rel(self, abs_pos):
        """
        Convert the absolute position in track to relative position in chromosome.

        Parameters
        ----------
        abs_pos: int
            absolute position in track

        Returns
        -------
        chromid: int
            index for the chromosome, starting from 1, return -1 if out of range
        rel_pos: int
            relative position in chromosome, starting from 0, return -1 if out of range

        """
        if abs_pos >= self.track_start_bp and abs_pos <= self.track_end_bp:
            rel_pos = abs_pos
            for chromid, end_bp in enumerate(self.chromo_end_bps[1:], 1):
                if end_bp >= abs_pos:
                    return chromid, rel_pos
                else:
                    rel_pos = abs_pos - end_bp
        return (-1, -1)

    def rel_to_abs(self, chromid, rel_pos):
        """
        Convert relative position in chromosome to absolute position in track.

        Parameters
        ----------
        chromid: int
            index for the chromo
        rel_pos: int
            relative position in chromosome

        Returns
        -------
        abs_pos: int
            absolute position in track

        """
        prev_end = self.chromo_end_bps[chromid-1] if chromid > 0 else 0
        return prev_end + rel_pos
