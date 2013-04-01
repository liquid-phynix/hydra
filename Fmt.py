class Fmt:
    """example use:
    Fmt('%a . %b . %c').sub(a = [1,2,3], b = [4,5,6]).sub(c = [7.8]) =>
    ['1 . 4 . 7', '1 . 4 . 8', '2 . 5 . 7', '2 . 5 . 8', '3 . 6 . 7', '3 . 6 . 8']"""
    def __init__(self, fmt):
        self.saturation, self.fmts = fmt.count('%'), [fmt.replace('%', '^')]
    def sub(self, **kwargs):
        trans_fmt = (functools.reduce(lambda s,k: s.replace('^'+k,'%('+k+')s'), kwargs, fmt) for fmt in self.fmts)
        repl = [dict(kwtpl) for kwtpl in zip(*[[(k,v) for v in vs] for k,vs in kwargs.items()])]
        self.fmts = [fmt % pdict for fmt in trans_fmt for pdict in repl]
        self.saturation -= len(kwargs)
        return self.fmts if self.saturation == 0 else self
    def __repr__(self):
        return '<\n' + ',\n'.join(self.fmts) + '\n>' 
