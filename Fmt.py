# import functools
# class Fmt:
#     """example use:
#     Fmt('%a . %b . %c').sub(a = [1,2,3], b = [4,5,6]).sub(c = [7.8]) =>
#     ['1 . 4 . 7', '1 . 4 . 8', '2 . 5 . 7', '2 . 5 . 8', '3 . 6 . 7', '3 . 6 . 8']"""
#     def __init__(self, fmt):
#         self.saturation, self.fmts = fmt.count('%'), [fmt.replace('%', '^')]
#     def sub(self, **kwargs):
#         trans_fmt = (functools.reduce(lambda s,k: s.replace('^'+k,'%('+k+')s'), kwargs, fmt) for fmt in self.fmts)
#         repl = [dict(kwtpl) for kwtpl in zip(*[[(k,v) for v in vs] for k,vs in kwargs.items()])]
#         self.fmts = [fmt % pdict for fmt in trans_fmt for pdict in repl]
#         self.saturation -= len(kwargs)
#         return self.fmts if self.saturation == 0 else self
#     def __repr__(self):
#         return '<\n' + ',\n'.join(self.fmts) + '\n>' 

class Args(object):
    """
    Args(2)(a = [1, 2, 3], b = [4, 5, 6])(c = [7, 8])
    """
    def __init__(self, n):
        self.n, self.args = n, [{}]
    def __call__(self, **kwargs):
        flat = [dict(kwtpl) for kwtpl in zip(*[[(k,v) for v in vs] for k,vs in kwargs.items()])]
        args = []
        for prev in self.args:
            for f in flat:
                _prev = prev.copy()
                _prev.update(f.copy())
                args += [_prev]
        self.args = args
        self.n -= 1
        if self.n <= 0: return self.args
        else: return self

def expand_jobs(fmt, args):
    """
    make_jobs('./pgm --arg0=%(arg0)s --arg1={arg1} --arg2={arg2} --arg3={arg3}',
              Args(2)(arg1=[1,2,3], arg2=[4,5,6])(arg3=[7,8]))
    """
    return [(fmt.format(**adict), adict) for adict in args]
