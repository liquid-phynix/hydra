import urwid
palette = [('body','black','dark cyan', 'standout'),
           ('foot','light gray', 'black'),
           ('key','dark red', 'black', 'underline'),
           ('title', 'white', 'black',)]
footer_text = [('title', 'Dynamic Pager'), ' | ',
               ('key', 'Up'), ', ', ('key', 'Down'), ', ',
               ('key', 'PgUp'), ', ', ('key', 'PgDown'),
               ' move view | ', ('key', 'Q'), ' exits']

def exit_on_q(input):
    if input in ('q', 'Q'): raise urwid.ExitMainLoop()

class Walker(urwid.ListWalker):
    def __init__(self, lines):
        self.focus, self.lines = 0, lines
        #        self.focus, self.lines, self.layout = 0, lines, Layout()
    def _get_at_pos(self, pos):
        if pos < 0 or pos >= len(self.lines):
            return None, None
        #            return urwid.Text(''), pos
        #            return urwid.Text('', layout = self.layout), pos
        #        else: return urwid.Text(self.lines[pos], layout = self.layout), pos
        else: return urwid.Text(self.lines[pos]), pos
    def get_focus(self): 
        return self._get_at_pos(self.focus)
    def set_focus(self, focus):
        self.focus = focus
        self._modified()
    def get_next(self, start_pos):
        return self._get_at_pos(start_pos + 1)
    def get_prev(self, start_pos):
        return self._get_at_pos(start_pos - 1)

# class Layout(urwid.TextLayout):
#     # def descend(self, line_start, text_len, width):
#     #     return [[(width, line_start, line_start + width)]] if text_len <= width else [[(width - 1, line_start, line_start + width - 1), (1, width - 1, '-')]] + self.descend(line_start + width - 1, text_len - width + 1, width)
#     def descend(self, line_start, text_len, width):
#         return [[(width, line_start, line_start + width)]] + ([] if text_len <= width else self.descend(line_start + width, text_len - width, width))
#     def layout(self, text, width, align, wrap):
#         return self.descend(0, len(text), width)

class Pager(urwid.MainLoop):
    def __init__(self, lines):
        self.listbox = urwid.ListBox(Walker(lines))
        self.footer = urwid.AttrMap(urwid.Text(footer_text), 'foot')
        self.view = urwid.Frame(urwid.AttrWrap(self.listbox, 'body'), footer = self.footer)
        super(Pager, self).__init__(self.view, palette, unhandled_input = exit_on_q)

# with open('server.py','r') as f:
#     sp = f.readlines()
