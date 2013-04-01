import urwid
palette = [('body','black','dark cyan', 'standout'),
           ('foot','light gray', 'black'),
           ('key','dark red', 'black', 'underline'),
           ('title', 'white', 'black',)]
footer_text = [('title', 'Dynamic Pager'), ' | ',
               ('key', 'Up'), ', ', ('key', 'Down'), ', ',
               ('key', 'PgUp'), ', ', ('key', 'PgDown'),
               ' move view | ', ('key', 'Q'), ' exits']

class Walker(urwid.ListWalker):
    def __init__(self, lines):
        self.focus, self.lines = 0, lines
        self.last_lines = [line for line in lines]
        self.last_len = len(self.last_lines)
        #        self.text = ''.join(str(2**n) for n in range(500)).split('1')
    def _get_at_pos(self, pos):
        # if pos < 0 or pos >= len(self.text): return None, None
        # else: return urwid.Text(self.text[pos]), pos
            #                   else: return urwid.Text(str(2**pos)), pos
        if pos < 0:
            return None, None
        elif pos >= self.last_len:
            newlen = len(self.lines)
            if newlen > self.last_len:
                lst = self.lines._getvalue()
                for i in range(self.last_len, newlen):
                    self.last_lines.append(lst[i])
                self.last_len = newlen
        if pos >= self.last_len:
            return None, None
        else:
            return urwid.Text(self.last_lines[pos]), pos
        # if pos < 0 or pos >= len(self.lines):
        #     return None, None
        # else: return urwid.Text(self.lines._getvalue()[pos]), pos
    def get_focus(self): 
        return self._get_at_pos(self.focus)
    def set_focus(self, focus):
        self.focus = focus
        self._modified()
    def get_next(self, start_pos):
        return self._get_at_pos(start_pos + 1)
    def get_prev(self, start_pos):
        return self._get_at_pos(start_pos - 1)

class Pager(urwid.MainLoop):
    def __init__(self, lines):
        self.lines = lines
        self.listbox = urwid.ListBox(Walker(lines))
        self.footer = urwid.AttrMap(urwid.Text(footer_text), 'foot')
        self.view = urwid.Frame(urwid.AttrWrap(self.listbox, 'body'), footer = self.footer)
        super(Pager, self).__init__(self.view, palette, unhandled_input = self.input_handler)
    def input_handler(self, input):
        if input in ('q', 'Q'): raise urwid.ExitMainLoop()
