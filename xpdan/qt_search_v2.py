import sys
from PyQt4 import QtGui, QtCore

class XpdanSerch(QtGui.QWidget):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("xpdAn search")
        self.setGeometry(1024, 1024, 400, 300)

        self.initUI()

    def initUI(self):
        #generate btn
        generate_btn = QtGui.QPushButton("generate query", self)
        generate_btn.clicked.connect(self.generate)
        generate_btn.resize(generate_btn.sizeHint())
        # reset btn
        reset_btn = QtGui.QPushButton("reset query", self)
        reset_btn.clicked.connect(self.reset)
        reset_btn.resize(reset_btn.sizeHint())

        # set up fileds
        self.f1 = QtGui.QLabel('PI last name')
        self.f1_Edit = QtGui.QLineEdit()
        self.f1_cbox = QtGui.QComboBox(self)
        self.f1_cbox.addItem("and")
        self.f1_cbox.addItem("or")

        self.f2 = QtGui.QLabel('sample name')
        self.f2_Edit = QtGui.QLineEdit()
        self.f2_cbox = QtGui.QComboBox(self)
        self.f2_cbox.addItem("and")
        self.f2_cbox.addItem("or")

        self.f3 = QtGui.QLabel('experiment name')
        self.f3_Edit = QtGui.QLineEdit()
        self.f3_cbox = QtGui.QComboBox(self)
        self.f3_cbox.addItem("and")
        self.f3_cbox.addItem("or")

        self.f4 = QtGui.QLabel('scan type')
        self.f4_Edit = QtGui.QLineEdit()
        self.f4_cbox = QtGui.QComboBox(self)
        self.f4_cbox.addItem("and")
        self.f4_cbox.addItem("or")


        grid = QtGui.QGridLayout()
        grid.setSpacing(5)

        grid.addWidget(self.f1, 1, 0)
        grid.addWidget(self.f1_Edit, 1, 1)
        grid.addWidget(self.f1_cbox, 1, 2)

        grid.addWidget(self.f2, 2, 0)
        grid.addWidget(self.f2_Edit, 2, 1)
        grid.addWidget(self.f2_cbox, 2, 2)

        grid.addWidget(self.f3, 3, 0)
        grid.addWidget(self.f3_Edit, 3, 1)
        grid.addWidget(self.f3_cbox, 3, 2)

        grid.addWidget(self.f4, 4, 0)
        grid.addWidget(self.f4_Edit, 4, 1)
        grid.addWidget(self.f4_cbox, 4, 2)

        # layout
        hbox = QtGui.QHBoxLayout()
        hbox.addStretch(1)
        hbox.addWidget(generate_btn)
        hbox.addWidget(reset_btn)

        vbox = QtGui.QVBoxLayout()
        vbox.addStretch(1)
        vbox.addLayout(hbox)
        vbox.addLayout(grid)

        self.setLayout(vbox)

        self.show()


    def generate(self):
        """ generate search query """
        print('Generate search query')
        print('f1={}'.format(self.f1_Edit.text()))


    def reset(self):
        """ reset search query """
        print('reset search query')

def main():
    app = QtGui.QApplication(sys.argv)
    gui = XpdanSerch()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
