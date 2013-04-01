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
    def _get_at_pos(self, pos):
        if pos < 0 or pos >= len(self.lines): return None, None
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

class Pager(urwid.MainLoop):
    def __init__(self, r):
        self.r = r
        self.lines = [] #[line.strip().decode() for line in self.r.readlines()]
        self.listbox = urwid.ListBox(Walker(self.lines))
        self.footer = urwid.AttrMap(urwid.Text(footer_text), 'foot')
        self.view = urwid.Frame(urwid.AttrWrap(self.listbox, 'body'), footer = self.footer)
        super(Pager, self).__init__(self.view, palette,
                                    unhandled_input = self.input_handler,
                                    handle_mouse = True)
        self.watch_file(r, self.update_from_fifo)
    def update_from_fifo(self):
        self.lines.append(self.r.readline().strip())
        #        self.draw_screen()
    def input_handler(self, input):
        if input in ('q', 'Q'):
            raise urwid.ExitMainLoop()
        elif input in ('f', 'F'):
            pass
        else:
            pass
