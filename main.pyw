import os
import sys
import socket
import logging, logging.handlers
import Queue
import itertools
import subprocess
import numpy
import gtk

import excepthook
from subproc_utils import ZMQServer, subprocess_with_queues
from subproc_utils.gtk_components import OutputBox

from LabConfig import LabConfig, config_prefix

import runmanager
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
                with gtk.gdk.lock:
                    success, message = app.receive_parameter_space(labscript_file, parameter_space)
                return success, message
            elif request_data[0] == 'from lyse':
                # A fitness reported from lyse:
                individual, fitness = request_data
                with gtk.gdk.lock:
                    success, message = app.report_fitness(individual, fitness)
                return success, message
        success, message = False, 'Request to mise not understood\n'
        return success, message
            

class Individual(object):
    counter = itertools.count()
    all_individuals = []
    
    def __init__(self, genome):
        self.genome = genome
        self.id = self.counter.next()
        self.fitness_visible = False
        self.fitness = None
        self.compile_progress_visible = True
        self.compile_progress = 0
        self.error_visible = None
        self.waiting_visible = False
        self.all_individuals.append(self)
        
    def __getitem__(self,item):
        return self.genome[item]
        
    
class Generation(object):
    counter = itertools.count()
    def __init__(self, population, parameters, previous_generation=None):
        self.id = self.counter.next()
        self.individuals = []
        if previous_generation is None:
            # Spawn individuals to create the first generation:
            for i in range(population):
                genome = {}
                for name, param in parameters.items():
                    if param.initial is None:
                        # Pick a random starting value within the range: 
                        value = numpy.random.rand()*(param.max-param.min) + param.min
                    else:
                        # Pick a value by applying one generation's worth
                        # of mutation to the initial value:
                        value = numpy.random.normal(param.initial, param.mutation_rate)
                        value = numpy.clip(value, param.min, param.max)
                    genome[name] = value
                individual = Individual(genome)
                self.individuals.append(individual)
        else:
            # Create a new generation from the previous one, by 'mating'
            # pairs of individuals with each other with a probability
            # based on their fitnesses.  First, we normalize the
            # fitnesses of previous generation to create a probability
            # mass function:
            fitnesses = numpy.array([individual.fitness for individual in previous_generation])
            fitnesses -= fitnesses.min()
            if fitnesses.max() != 0:
                fitnesses /= fitnesses.max()
            # Add an offset to ensure that the least fit individual
            # will still have a nonzero probability of reproduction;
            # approx 1/N times the most fit individual's probability:
            fitnesses += 1/len(fitnesses)
            fitnesses /= fitnesses.sum()
            # Let mating season begin:
            while len(self.individuals) < population:
                # Pick parent number #1
                parent_1_index = numpy.searchsorted(cumsum(fitnesses), numpy.random.rand())
                # Pick parent number #2, must be different to parent #1:
                parent_2_index = parent_1_index
                while parent_2_index == parent_1_index:
                    parent_2_index = numpy.searchsorted(cumsum(fitnesses), numpy.random.rand())
                parent_1 = previous_generation[parent_1_index]
                parent_2 = previous_generation[parent_2_index]
                # Now we have two parents. Let's mix their genomes:
                child_genome = {}
                for name, param in parameters.items():
                    if 'name' in parent_1 and 'name' in parent_2:
                        # Pick a value for this parameter from a uniform
                        # probability distribution between it's parents'
                        # values:
                        lim1, lim2 = parent_1[name], parent_2[name]
                        child_value = numpy.random.rand()*(lim2-lim1) + lim1
                        # Apply a Gaussian mutation and clip to keep in limits:
                        child_value = numpy.random.normal(child_value, param.mutation_rate)
                        child_value = numpy.clip(child_value, param.min, param.max)
                    else:
                        # The parents don't have this
                        # parameter. Parameters must have changed,
                        # we need an initial value for this parameter:
                        if param.initial is None:
                            # Pick a random starting value within the range: 
                            child_value = numpy.random.rand()*(param.max-param.min) + param.min
                        else:
                            # Pick a value by applying one generation's worth
                            # of mutation to the initial value:
                            child_value = numpy.random.normal(param.initial, param.mutation_rate)
                            child_value = numpy.clip(value, param.min, param.max)
                    child_genome[name] = child_value
                    
                # Congratulations, it's a boy!
                child = Individual(genome)
                self.individuals.append(child)
                    
    def __iter__(self):
        return iter(self.individuals)
        
    def __getitem__(self, index):
        return self.individuals[index]
            

class Mise(object):

    base_liststore_cols = ['generation', 
                           'id',
                           'fitness_visible',
                           'fitness',
                           'compile_progress_visible',
                           'compile_progress',
                           'error_visible',
                           'waiting_visible']
    
    base_liststore_types = {'generation': str, 
                            'id': str,
                            'fitness_visible': bool,
                            'fitness': str,
                            'compile_progress_visible': bool,
                            'compile_progress': int,
                            'error_visible': bool,
                            'waiting_visible': bool}
                            
    def __init__(self):
    
        # Make a gtk Builder with the user interface file:
        builder = gtk.Builder()
        builder.add_from_file('main.glade')
        
        # Get required objects from the builder:
        outputbox_container = builder.get_object('outputbox_container')
        self.window = builder.get_object('window')
        self.liststore_parameters = builder.get_object('liststore_parameters')
        self.treeview_individuals = builder.get_object('treeview_individuals')
        
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
        logger.info('starting web server on port %s'%port)
        self.server = WebServer(port)
    
        self.params = {}
        self.labscript_file = None
        
        self.population = 10
        self.current_generation = None
        self.generations = []
        
        self.new_individual_liststore()
        
        # Start the compiler subprocess:
        runmanager_dir=os.path.dirname(runmanager.__file__)
        batch_compiler = os.path.join(runmanager_dir, 'batch_compiler.py')
        self.to_child, self.from_child, child = subprocess_with_queues(batch_compiler)

        logger.info('init done')
    
    def destroy(self, widget):
        logger.info('destroy')
        gtk.main_quit()
            
    def receive_parameter_space(self, labscript_file, parameter_space):
        """Receive a parameter space dictionary from runmanger"""
        self.params = {}
        self.labscript_file = labscript_file
        self.liststore_parameters.clear()
        # Pull out the MiseParameters:
        for name, value in parameter_space.items():
            if isinstance(value, MiseParameter):
                data = [name, value.min, value.max, value.mutation_rate, value.log]
                self.liststore_parameters.append(data)
                self.params[name] = value
        if self.current_generation is None:
            self.current_generation = Generation(self.population, self.params)
            self.generations.append(self.current_generation)
        self.new_individual_liststore()
        self.parameter_space = parameter_space
        return True, 'optimisation request added successfully\n'

    def report_fitness(self, individual, fitness):
        print individual, fitness
        return True, 'dummy message\n'
        
    def new_individual_liststore(self):
        column_names = self.base_liststore_cols + self.params.keys()
        column_types = [self.base_liststore_types[name] for name in self.base_liststore_cols]  + [str for name in self.params]
        self.liststore_individuals = gtk.ListStore(*column_types)
        self.treeview_individuals.set_model(self.liststore_individuals)
        for generation in self.generations:
            for individual in generation:
                row = [generation.id, 
                       individual.id, 
                       individual.fitness_visible, 
                       individual.fitness,
                       individual.compile_progress_visible,
                       individual.compile_progress,
                       individual.error_visible,
                       individual.waiting_visible]
                row += [individual[name] for name in self.params]
                self.liststore_individuals.append(row)
                
    def mainloop(self):
        pass
        
if __name__ == '__main__':
    gtk.threads_init()
    app = Mise()
    with gtk.gdk.lock:
        gtk.main()    
