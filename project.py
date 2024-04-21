from interface import App
from interface import InnerState

"""
The entry point of the project that triggers the mainloop.
"""
if __name__ == '__main__':
    inner_state = InnerState()
    app = App(inner_state)
    app.mainloop()
