from interface import App
from interface import InnerState

if __name__ == '__main__':
    inner_state = InnerState()
    app = App(inner_state)
    app.mainloop()
