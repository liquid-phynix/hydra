import urwid
palette = [('body','black','dark cyan', 'standout'),
           ('foot','light gray', 'black'),
           ('key','dark red', 'black', 'underline'),
           ('title', 'white', 'black',)]
mode = 'Static'
footer_text = [('title', 'Dynamic Pager'), ' | ',
               ('key', 'Up'), ', ', ('key', 'Down'), ', ',
               ('key', 'PgUp'), ', ', ('key', 'PgDown'),
               ' move view | ', ('key', 'Q'), ' exits', ' | ', 'Mode ' , ('key', mode)]

class Pager(urwid.MainLoop):
    def __init__(self, queue):
        self.queue = queue
        lines = []
        while not queue.empty():
            lines.append(queue.get())
        self.lw = urwid.SimpleListWalker([urwid.Text(line) for line in lines])
        self.listbox = urwid.ListBox(self.lw)
        self.footer_text_text = urwid.Text(footer_text)
        self.footer = urwid.AttrMap(self.footer_text_text, 'foot')
        self.view = urwid.Frame(urwid.AttrWrap(self.listbox, 'body'), footer = self.footer)
        super(Pager, self).__init__(self.view, palette,
                                    unhandled_input = self.input_handler,
                                    handle_mouse = True)
        self.set_alarm_in(0.05, self.callback)
    def callback(self, mobj, udata):
        changed = False
        while not self.queue.empty():
            self.lw.append(urwid.Text(self.queue.get()))
            changed = True
        if changed and mode == 'Follow':
            self.lw.set_focus(len(self.lw) - 1)
        self.set_alarm_in(0.05, self.callback)
    def input_handler(self, input):
        global mode
        if input in ('q', 'Q'):
            raise urwid.ExitMainLoop()
        elif input in ('f', 'F'):
            if mode == 'Static':
                mode = 'Follow'
                self.footer_text_text.set_text(footer_text)
                self.draw_screen()
        else:
            print('%s pressed' % input)
            if mode == 'Follow':
                mode = 'Static'
                self.footer_text_text.set_text(footer_text)
                self.draw_screen()
# class Walker(urwid.SimpleFocusListWalker):
#     def __init__(self, lines):
#         self.focus, self.lines = 0, lines
#     def _get_at_pos(self, pos):
#         if pos < 0 or pos >= len(self.lines): return None, None
#         else: return urwid.Text(self.lines[pos]), pos
#     def get_focus(self): 
#         return self._get_at_pos(self.focus)
#     def set_focus(self, focus):
#         self.focus = focus
#         self._modified()
#     def get_next(self, start_pos):
#         return self._get_at_pos(start_pos + 1)
#     def get_prev(self, start_pos):
#         return self._get_at_pos(start_pos - 1)
