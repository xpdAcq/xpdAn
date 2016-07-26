import sys
import time
import datetime
from PyQt4 import QtGui, QtCore

from .xpdan import XpdAn
an = XpdAn(saf_num=123) # dummy init

class XpdanSearch(QtGui.QWidget):

    FIELD_LIST = ['bt_piLast', 'sa_name','bt_experimenters',
                  'plan_name']

    MD_FIELD_LIST = ['bt_piLast', 'sa_name', 'sp_computed_exposure',
                     'time', 'plan_name']

    def __init__(self):
        super().__init__()
        self._an = an # handing object

        # option_list
        self.field_option_list = []

        # md field_list
        self.md_field_list = []

        self.initUI()

    def initUI(self):

        #generate btn
        generate_btn = QtGui.QPushButton("generate query and search", self)
        generate_btn.clicked.connect(self.generate)
        generate_btn.resize(generate_btn.sizeHint())
        #FIXME------ shortcut doesn't work
        fireSearch = QtGui.QAction(self)
        fireSearch.setShortcut('enter')
        fireSearch.triggered.connect(self.generate)

        # set up search fileds
        self.f1 = QtGui.QLabel('PI last name')
        self.f1_chkbox = QtGui.QCheckBox(self)
        self.f1_Edit = QtGui.QLineEdit()
        self.f1_cbox = QtGui.QComboBox(self)
        self.f1_cbox.addItem("and")
        self.f1_cbox.addItem("or")
        self.f1_fuzzy = QtGui.QCheckBox('starts with')
        self.field_option_list.append((self.f1_chkbox, self.f1_Edit,
                                       self.f1_cbox, self.f1_fuzzy))
        #self.f1_cbox.activated[str].connect(self.f1_and_or_option)

        self.f2 = QtGui.QLabel('sample name')
        self.f2_chkbox = QtGui.QCheckBox(self)
        self.f2_Edit = QtGui.QLineEdit()
        self.f2_cbox = QtGui.QComboBox(self)
        self.f2_cbox.addItem("and")
        self.f2_cbox.addItem("or")
        self.f2_fuzzy = QtGui.QCheckBox('starts with')
        self.field_option_list.append((self.f2_chkbox, self.f2_Edit,
                                       self.f2_cbox, self.f2_fuzzy))

        self.f3 = QtGui.QLabel('experimenter name')
        self.f3_chkbox = QtGui.QCheckBox(self)
        self.f3_Edit = QtGui.QLineEdit()
        self.f3_cbox = QtGui.QComboBox(self)
        self.f3_cbox.addItem("and")
        self.f3_cbox.addItem("or")
        self.f3_fuzzy = QtGui.QCheckBox('starts with')
        self.field_option_list.append((self.f3_chkbox, self.f3_Edit,
                                       self.f3_cbox, self.f3_fuzzy))

        self.f4 = QtGui.QLabel('scan type')
        self.f4_chkbox = QtGui.QCheckBox(self)
        self.f4_Edit = QtGui.QLineEdit()
        self.f4_cbox = QtGui.QComboBox(self)
        self.f4_cbox.addItem("and")
        self.f4_cbox.addItem("or")
        self.f4_fuzzy = QtGui.QCheckBox('starts with')
        self.field_option_list.append((self.f4_chkbox, self.f4_Edit,
                                       self.f4_cbox, self.f4_fuzzy))

        self.output = QtGui.QLabel('Search Query History')
        self.output_box = QtGui.QTextEdit()
        self.output_box.setReadOnly(True)
        self.output_box.moveCursor(QtGui.QTextCursor.End)
        self.output_box.setLineWrapMode(self.output_box.NoWrap)
        # this is default supported in databroker
        #self.datakey = QtGui.QLabel('data key')
        #self.datakey_Edit = QtGui.QLineEdit()
        #self.datakey_cbox = QtGui.QComboBox(self)
        #self.datakey_cbox.addItem("and")
        #self.datakey_cbox.addItem("or")
        #self.datakey_cbox.addItem("in")
        #self.field_option_list.append((self.datakey_Edit, self.datakey_cbox))

        self.date_range_option_list = []
        self.start_date_chkbox = QtGui.QCheckBox(self)
        self.start_date_label = QtGui.QLabel('start date')
        self.start_date = QtGui.QCalendarWidget()
        self.start_date.setGridVisible(True)
        self.start_date_cbox = QtGui.QComboBox(self)
        self.start_date_cbox.addItem("and")
        #self.start_date_cbox.addItem("or")
        self.date_range_option_list.append((self.start_date_chkbox,
                                            self.start_date,
                                            self.start_date_cbox))
        #self.start_date.clicked[QtCore.QDate].connect(self.getDate)

        self.end_date_chkbox = QtGui.QCheckBox(self)
        self.end_date_label = QtGui.QLabel('end date')
        self.end_date = QtGui.QCalendarWidget()
        self.end_date.setGridVisible(True)
        self.end_date_cbox = QtGui.QComboBox(self)
        self.end_date_cbox.addItem("and")
        #self.end_date_cbox.addItem("or")
        self.date_range_option_list.append((self.end_date_chkbox,
                                            self.end_date,
                                            self.end_date_cbox))
        #self.end_date.clicked[QtCore.QDate].connect(self.getDate)


        # field to display md
        #MD_FIELD_LIST = ['bt_piLast', 'sa_name', 'sp_computed_exposure',
        #                 'time', 'plan_name']
        self.md_chkbox_1 = QtGui.QCheckBox('PI last name')
        self.md_field_list.append(self.md_chkbox_1)
        self.md_chkbox_2 = QtGui.QCheckBox('sample name' )
        self.md_field_list.append(self.md_chkbox_2)
        self.md_chkbox_3 = QtGui.QCheckBox('total exposure time')
        self.md_field_list.append(self.md_chkbox_3)
        self.md_chkbox_4 = QtGui.QCheckBox('time' )
        self.md_field_list.append(self.md_chkbox_4)
        self.md_chkbox_5 = QtGui.QCheckBox('plan name' )
        self.md_field_list.append(self.md_chkbox_5)

        # group widgets
        grid = QtGui.QGridLayout()
        grid.setSpacing(5)

        grid.addWidget(self.f1_chkbox, 1, 0)
        grid.addWidget(self.f1, 1, 1)
        grid.addWidget(self.f1_Edit, 1, 2)
        grid.addWidget(self.f1_cbox, 1, 3)
        grid.addWidget(self.f1_fuzzy, 1, 4)

        grid.addWidget(self.f2_chkbox, 2, 0)
        grid.addWidget(self.f2, 2, 1)
        grid.addWidget(self.f2_Edit, 2, 2)
        grid.addWidget(self.f2_cbox, 2, 3)
        grid.addWidget(self.f2_fuzzy, 2, 4)

        grid.addWidget(self.f3_chkbox, 3, 0)
        grid.addWidget(self.f3, 3, 1)
        grid.addWidget(self.f3_Edit, 3, 2)
        grid.addWidget(self.f3_cbox, 3, 3)
        grid.addWidget(self.f3_fuzzy, 3, 4)

        grid.addWidget(self.f4_chkbox, 4, 0)
        grid.addWidget(self.f4, 4, 1)
        grid.addWidget(self.f4_Edit, 4, 2)
        grid.addWidget(self.f4_cbox, 4, 3)
        grid.addWidget(self.f4_fuzzy, 4, 4)

        grid.addWidget(self.start_date_chkbox, 5, 0)
        grid.addWidget(self.start_date_label, 5, 1)
        grid.addWidget(self.start_date, 5, 2)
        grid.addWidget(self.start_date_cbox, 5, 3)

        grid.addWidget(self.end_date_chkbox, 6, 0)
        grid.addWidget(self.end_date_label, 6, 1)
        grid.addWidget(self.end_date, 6, 2)
        grid.addWidget(self.end_date_cbox, 6, 3)

        #grid.addWidget(self.datakey, 5, 0)
        #grid.addWidget(self.datakey_Edit, 5, 1)
        #grid.addWidget(self.datakey_cbox, 5, 2)

        md_grid = QtGui.QGridLayout()
        md_grid.setSpacing(5)
        md_grid.addWidget(QtGui.QLabel('Metadata fields to display'), 1, 0)
        for i in range(len(self.md_field_list)):
            md_grid.addWidget(self.md_field_list[i], 2, i)

        out_grid = QtGui.QGridLayout()
        out_grid.setSpacing(5)

        out_grid.addWidget(self.output, 1, 0)
        out_grid.addWidget(self.output_box, 2, 0)

        # configure layout
        hbox = QtGui.QHBoxLayout()
        hbox.addStretch(1)
        hbox.addWidget(generate_btn)

        vbox = QtGui.QVBoxLayout()
        vbox.addStretch(1)
        vbox.addLayout(grid)
        vbox.addLayout(md_grid)
        vbox.addLayout(hbox)
        vbox.addLayout(out_grid)

        self.setLayout(vbox)

        self.setWindowTitle("xpdAn search")
        self.setGeometry(2048, 0, 400, 600)

        self.show()

    def getDate(self, date):
        print(date.toString())

    def date2timestamp(self, obj):
        selected_date = obj.selectedDate().toPyDate()
        return time.mktime(selected_date.timetuple())

    def generate(self):
        """ generate search query """
        #print('Generate search query')
        search_dict = {'group':'XPD'}
        or_list = [] # query purpose
        in_list = [] # ref

        # date range
        time_dict = {}
        gtlt_dict = {}
        if self.start_date_chkbox.isChecked():
            start_time = self.date2timestamp(self.start_date)
            gtlt_dict.update({'$gte':start_time})
        else:
            pass
        if self.end_date_chkbox.isChecked():
            end_time = self.date2timestamp(self.end_date)
            gtlt_dict.update({'$lte':end_time})
        else:
            pass
        if gtlt_dict:
            time_dict.update({'time':gtlt_dict})
            search_dict.update(time_dict)

        # key-val search
        for i in range(len(self.FIELD_LIST)):
            key = self.FIELD_LIST[i]
            y_n = self.field_option_list[i][0]
            val = self.field_option_list[i][1].text().split(',')
            option = self.field_option_list[i][2].currentText()
            fuzzy = self.field_option_list[i][3]
            if y_n.isChecked():
                if len(val) ==1 and val != ['']:
                    _val = val[0].strip()
                    # update fuzzy search
                    if fuzzy.isChecked():
                        _val = '/^{}/'.format(_val)
                    # update logic
                    if option == 'or':
                        or_list.append({key:_val})
                    else: # and-logic
                        search_dict.update({key:val[0].strip()})
                elif len(val) >1:
                    for el in val:
                        _val = el.strip()
                        if fuzzy.isChecked():
                            _val = '/^{}/'.format(_val)
                        or_list.append({key:_val})
                else:
                    # + is selected but no field entered, skipped
                    pass

        # update logic
        if or_list:
            search_dict.update({'$or':or_list})

        # validate search key
        if len(search_dict) < 2:
            self.output_box.insertPlainText("Warning: at least need one"
                                            " key\n")
            return

        # show query history
        self.output_box.insertPlainText('{}\n'.format(search_dict))
        sb = self.output_box.verticalScrollBar()
        sb.setValue(sb.maximum())

        # auto load to clippboard
        #cb = QtGui.QApplication.clipboard()
        #cb.clear(mode=cb.Clipboard )
        #cb.setText('{}'.format(search_dict), mode=cb.Clipboard)

        # operate on XpdAn
        self._an.search_dict = search_dict
        md_field = []
        #print('length of md_field_list = {}'.format(len(self.md_field_list)))
        #print('length of MD_FIELD_LIST = {}'.format(len(self.MD_FIELD_LIST)))
        for i in range(len(self.md_field_list)):
            el = self.md_field_list[i]
            if el.isChecked():
                md_field.append(self.MD_FIELD_LIST[i])
        if md_field: # other than default
            self._an.header_md_fields = md_field
        self._an.list()
"""
def main():
    app = QtGui.QApplication(sys.argv)
    gui = XpdanSerch()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
"""
