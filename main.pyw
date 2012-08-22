import os
import sys
import socket
import logging, logging.handlers
import Queue

import gtk

import excepthook
from subproc_utils import ZMQServer
from subproc_utils.gtk_components import OutputBox

from LabConfig import LabConfig, config_prefix

from mise import MiseParameter

# This provides debug info without having to run from a terminal, and
# avoids a stupid crash on Windows when there is no command window:
if not sys.stdout.isatty():
    sys.stdout = sys.stderr = open('debug.log','w',1)
    
if os.name == 'nt':
    # Make it not look so terrible (if icons and themes are installed):
    gtk.settings_get_default().set_string_property('gtk-icon-theme-name','gnome-human','')
    gtk.settings_get_default().set_string_property('gtk-theme-name','Clearlooks','')
    gtk.settings_get_default().set_string_property('gtk-font-name','ubuntu 11','')
    gtk.settings_get_default().set_long_property('gtk-button-images',False,'')

    # Have Windows 7 consider this program to be a separate app, and not
    # group it with other Python programs in the taskbar:
    import ctypes
    myappid = 'monashbec.labscript.mise' # arbitrary string
    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
    except:
        pass

def setup_logging():
    logger = logging.getLogger('mise')
    handler = logging.handlers.RotatingFileHandler(r'mise.log', maxBytes=1024*1024*50)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    if sys.stdout.isatty():
        terminalhandler = logging.StreamHandler(sys.stdout)
        terminalhandler.setFormatter(formatter)
        terminalhandler.setLevel(logging.DEBUG) # only display info or higher in the terminal
        logger.addHandler(terminalhandler)
    logger.setLevel(logging.DEBUG)
    return logger
    
logger = setup_logging()
excepthook.set_logger(logger)
logger.info('\n\n===============starting===============\n')


class WebServer(ZMQServer):
    """A server to receive parameter spaces from runmanager, and fitness
    reporting from lyse"""
    def handler(self, request_data):
        if request_data == 'hello':
            # just a ping:
            return 'hello'
        elif isinstance(request_data,tuple) and len(request_data) > 1:
            if request_data[0] == 'from runmanager':
                # A parameter space from runmanager:
                labscript_file, parameter_space = request_data[1:]
                success, message = app.receive_parameter_space(labscript_file, parameter_space)
                return success, message
            elif request_data[0] == 'from lyse':
                # A fitness reported from lyse:
                individual, fitness = request_data
                success, message = app.report_fitness(individual, fitness)
                return success, message
        success, message = False, 'Request to mise not understood\n'
        return success, message
            
            
class Mise(object):
    def __init__(self):
    
        # Make a gtk Builder with the user interface file:
        builder = gtk.Builder()
        builder.add_from_file('main.glade')
        
        # Get required objects from the builder:
        outputbox_container = builder.get_object('outputbox_container')
        self.window = builder.get_object('window')
        
        # Connect signals:
        builder.connect_signals(self)
        
        # Show the main window:
        self.window.show()
        
        # Compilations will have their output streams
        # redirected to the outputbox via a queue:
        self.to_outputbox = Queue.Queue()
        
        # Make an output box for terminal output:
        outputbox = OutputBox(outputbox_container, self.to_outputbox)
        
        # Get settings:
        config_path = os.path.join(config_prefix,'%s.ini'%socket.gethostname())
        required_config_params = {"paths":["experiment_shot_storage"],'ports':['mise']}
        self.config = LabConfig(config_path,required_config_params)

        # Start the web server:
        port = self.config.get('ports','mise')
        self.server = WebServer(port)
    
        self.mised_params = []
        logger.info('init done')
    
    def destroy(self, widget):
        print 'destroy!'
            
    def receive_parameter_space(self, labscript_file, parameter_space):
        """Receive a parameter space dictionary from runmanger"""
        mised_params = []
        for key, value in parameter_space.items():
            if isinstance(value, MiseParameter):
                mised_params.append(value)
        print mised_params
        return True, 'dummy message\n'
    
    def report_fitness(self, individual, fitness):
        print individual, fitness
        return True, 'dummy message\n'
        
if __name__ == '__main__':
    gtk.threads_init()
    app = Mise()
    with gtk.gdk.lock:
        gtk.main()    
