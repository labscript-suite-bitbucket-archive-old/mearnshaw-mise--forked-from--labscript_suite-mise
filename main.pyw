import os
import sys
import socket
import logging, logging.handlers
import Queue
import itertools
import subprocess
import threading
import numpy
import gtk
import urllib, urllib2

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
                runmanager_data = request_data[1:]
                print len(runmanager_data),'************'
                with gtk.gdk.lock:
                    success, message = app.receive_parameter_space(runmanager_data)
                return success, message
            elif request_data[0] == 'from lyse':
                # A fitness reported from lyse:
                individual, fitness = request_data
                with gtk.gdk.lock:
                    success, message = app.report_fitness(individual, fitness)
                return success, message
        success, message = False, 'Request to mise not understood\n'
        return success, message
            
class IndividualNotFound(Exception):
    """An exception class for when an operation on an individual fails
    because the individual has been deleted in the meantime."""
    pass
    
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

# Some convenient constants for accessing liststore columns:   

# Individual list store:       
GENERATION = 0
ID = 1
FITNESS_VISIBLE = 2
FITNESS = 3
COMPILE_PROGRESS_VISIBLE = 4
COMPILE_PROGRESS = 5
ERROR_VISIBLE = 6
WAITING_VISIBLE = 7
    
# Parameter liststore:
NAME = 0
MIN = 1
MAX = 2
MUTATION_RATE = 3
LOG = 4
 
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
        self.pause_button = builder.get_object('pause_button')
        self.box_paused = builder.get_object('paused')
        self.box_not_paused = builder.get_object('not_paused')
        self.label_labscript_file = builder.get_object('label_labscript_file')
        self.label_output_directory = builder.get_object('label_output_directory')
        
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
    
        # A condition to let the looping threads know when to recheck conditions
        # they're waiting on (instead of having them do time.sleep)
        self.timing_condition = threading.Condition()
        
        self.params = {}
        self.labscript_file = None
        
        self.population = 10
        self.current_generation = None
        self.generations = []
        
        self.treeview_parameter_columns = []
        self.new_individual_liststore()
        
        # Start the compiler subprocess:
        runmanager_dir=os.path.dirname(runmanager.__file__)
        batch_compiler = os.path.join(runmanager_dir, 'batch_compiler.py')
        self.to_child, self.from_child, child = subprocess_with_queues(batch_compiler)

        self.paused = False
        
        # A thread which looks for un-compiled individuals and compiles
        # them, submitting them to BLACS:
        self.compile_thread = threading.Thread(target=self.compile_loop)
        self.compile_thread.daemon = True
        self.compile_thread.start()
        
        # A thread which looks for when all fitnesses have come back,
        # and spawns a new generation when they have:
        self.reproduction_thread = threading.Thread(target=self.reproduction_loop)
        self.reproduction_thread.daemon = True
        self.reproduction_thread.start()
        
        logger.info('init done')
    
    def destroy(self, widget):
        logger.info('destroy')
        gtk.main_quit()
    
    def error_dialog(self, message):
        dialog =  gtk.MessageDialog(self.window, gtk.DIALOG_DESTROY_WITH_PARENT, gtk.MESSAGE_WARNING, 
                                    buttons=(gtk.BUTTONS_OK), message_format = message)
        result = dialog.run()
        dialog.destroy()
           
    def on_pause_button_toggled(self,button):
        if button.get_active():
            self.paused = True
            self.box_paused.show()
            self.box_not_paused.hide()
        else:
            self.paused = False
            self.box_paused.hide()
            self.box_not_paused.show()
            with self.timing_condition:
                self.timing_condition.notify_all()
    
    def on_parameter_min_edited(self, renderer, rowindex, value):
        row = self.liststore_parameters[int(rowindex)]
        name = row[NAME]
        param = self.params[name]
        try:
            value = float(eval(value))
        except Exception as e:
            self.error_dialog(str(e))
            return
        if value >= param.max:
            self.error_dialog('Must have min < max.')
            return
        param.min = value
        row[MIN] = value
    
    def on_parameter_max_edited(self, renderer, rowindex, value):
        row = self.liststore_parameters[int(rowindex)]
        name = row[NAME]
        param = self.params[name]
        try:
            value = float(eval(value))
        except Exception as e:
            self.error_dialog(str(e))
            return
        if value <= param.min:
            self.error_dialog('Must have max > min.')
            return
        param.max = value
        row[MAX] = value
        
    def on_parameter_mutationrate_edited(self, renderer, rowindex, value):
        row = self.liststore_parameters[int(rowindex)]
        name = row[NAME]
        param = self.params[name]
        try:
            value = float(eval(value))
        except Exception as e:
            self.error_dialog(str(e))
            return
        param.mutation_rate = value
        row[MUTATION_RATE] = value
    
    def on_parameter_logarithmic_toggled(self, renderer, rowindex):
        row = self.liststore_parameters[int(rowindex)]
        name = row[NAME]
        param = self.params[name]
        param.log = not param.log
        row[LOG] = param.log
                   
    def receive_parameter_space(self, runmanager_data):
        """Receive a parameter space dictionary from runmanger"""
        (labscript_file, sequenceglobals, shots, 
             output_folder, shuffle, BLACS_server, BLACS_port, shared_drive_prefix) = runmanager_data
        self.params = {}
        self.liststore_parameters.clear()
        # Pull out the MiseParameters:
        first_shot = shots[0]
        for name, value in first_shot.items():
            if isinstance(value, MiseParameter):
                data = [name, value.min, value.max, value.mutation_rate, value.log]
                self.liststore_parameters.append(data)
                self.params[name] = value
        if self.current_generation is None:
            self.current_generation = Generation(self.population, self.params)
            self.generations.append(self.current_generation)
        self.new_individual_liststore()
        self.labscript_file = labscript_file
        self.sequenceglobals = sequenceglobals
        self.shots = shots
        self.output_folder = output_folder
        self.shuffle = shuffle
        self.BLACS_server = BLACS_server
        self.BLACS_port = BLACS_port
        self.shared_drive_prefix = shared_drive_prefix
            
        self.label_labscript_file.set_text(self.labscript_file)
        self.label_output_directory.set_text(self.output_folder)
        # Let waiting threads know that there might be new state for them to check:
        with self.timing_condition:
            self.timing_condition.notify_all()
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
        # Make sure the Treeview has columns for the current parameters:
        for param_name in self.params:
            if not param_name in self.treeview_parameter_columns:
                self.treeview_parameter_columns.append(param_name)
                model_column_index = column_names.index(param_name)
                renderer = gtk.CellRendererText()
                widget = gtk.HBox()
                heading = gtk.Label(param_name)
                heading.show()
                column = gtk.TreeViewColumn()
                column.pack_start(renderer)
                column.set_widget(heading)
                column.add_attribute(renderer, 'text', model_column_index)
                column.set_resizable(True)
                column.set_reorderable(True)
                self.treeview_individuals.append_column(column)
                
    def set_value(self, individual, column, value):
        """Searches the liststore for the individual, setting the
        value of a particular column in the individual's row. Raises
        IndividualNotFound if the row is not found. You must acquire
        the gtk lock before calling this method."""
        for row in self.liststore_individuals:
            if int(row[ID]) == individual.id:
                row[column] = value
                return
        raise IndividualNotFound
                        
    def compile_one_individual(self,individual):
        # Create a list of shot globals for this individual, by copying
        # self.shots and replacing MiseParameters with their values for
        # this individual:
        shots = []
        for shot in self.shots:
            this_shot = shot.copy()
            for param_name in individual.genome:
                this_shot[param_name] = individual[param_name]
            shots.append(this_shot)
        # Create run files:
        sequence_id = runmanager.generate_sequence_id(self.labscript_file) + '_g%di%d'%(self.current_generation.id, individual.id)
        n_run_files = len(shots)
        try:
            run_files = runmanager.make_run_files(self.output_folder, self.sequenceglobals, shots, sequence_id, self.shuffle)
            with gtk.gdk.lock:
                individual.error_visible = False
                self.set_value(individual, ERROR_VISIBLE, individual.error_visible)
            for i, run_file in enumerate(run_files):
                self.to_child.put(['compile',[self.labscript_file,run_file]])
                while True:
                    signal,data = self.from_child.get()
                    if signal in ['stdout','stderr']:
                        self.to_outputbox.put([signal,data])
                    elif signal == 'done':
                        success = data
                        break
                if not success:
                    break
                else:
                    with gtk.gdk.lock:
                        individual.compile_progress = 100*float(i+1)/n_run_files
                        self.set_value(individual, COMPILE_PROGRESS, individual.compile_progress)
                        if individual.compile_progress == 100:
                            individual.compile_progress_visible = False
                            self.set_value(individual, COMPILE_PROGRESS_VISIBLE, individual.compile_progress_visible)
                            individual.waiting_visible = True
                            self.set_value(individual, WAITING_VISIBLE, individual.waiting_visible)
                    self.submit_job(run_file)
                    
        except IndividualNotFound:
            # The Individial has been deleted at some point. It's gone,
            # so we don't have to worry about where we were up to with
            # anything. It will be garbage collected....now:
            return
            
        except Exception as e :
            # Couldn't make or run files, couldn't compile, or couldn't
            # submit. Print the error, pause mise, and display error icon:
            self.to_outputbox.put(['stderr', str(e) + '\n'])
            with gtk.gdk.lock:
                self.pause_button.set_active(True)
                individual.compile_progress = 0
                self.set_value(individual, COMPILE_PROGRESS, individual.compile_progress)
                individual.compile_progress_visible = False
                self.set_value(individual, COMPILE_PROGRESS_VISIBLE, individual.compile_progress_visible)
                individual.error_visible = True
                self.set_value(individual, ERROR_VISIBLE, individual.error_visible)
                individual.waiting_visible = False
                self.set_value(individual, WAITING_VISIBLE, individual.waiting_visible)
            
   
    def submit_job(self, run_file):
        # Workaround to force python not to use IPv6 for the request:
        address  = socket.gethostbyname(self.BLACS_server)
        run_file = run_file.replace(self.shared_drive_prefix,'Z:/').replace('/','\\')
        self.to_outputbox.put(['stdout','Submitting run file %s.\n'%os.path.basename(run_file)])
        params = urllib.urlencode({'filepath': run_file})
        try:
            response = urllib2.urlopen('http://%s:%d'%(address,self.BLACS_port), params, 2).read()
            if 'added successfully' in response:
                self.to_outputbox.put(['stdout',response])
            else:
                raise Exception(response)
        except Exception:
            self.to_outputbox.put(['stderr', 'Couldn\'t submit job to control server:\n'])
            raise         
            
    def compile_loop(self):
        while True:
            while self.paused:
                with self.timing_condition:
                    self.timing_condition.wait()
            logger.info('compile loop iteration')
            # Get the next individual requiring compilation:
            individual = None
            with gtk.gdk.lock:
                for row in self.liststore_individuals:
                    if row[COMPILE_PROGRESS] == 0:
                        individual_id = int(row[ID])
                        individual = Individual.all_individuals[individual_id]
                        logger.info('individual %d needs compiling'%individual_id)
                        break
            # If we didn't find any individuals requiring compilation,
            # wait until a timing_condition notification before checking
            # again:
            if individual is None:
                logger.info('no individuals requiring compilation')
                with self.timing_condition:
                    self.timing_condition.wait()
                    continue
            # OK, we have an individual which requires compilation.
            self.compile_one_individual(individual)
                    
    def reproduction_loop(self):
        while True:
            while self.paused or self.current_generation is None:
                with self.timing_condition:
                    self.timing_condition.wait()
            logger.info('reproduction loop iteration')
            if not all([individual.fitness is not None for individual in self.current_generation]):
                # Still waiting on at least one individual, do not spawn a new generation yet
                with self.timing_condition:
                    self.timing_condition.wait()
                
if __name__ == '__main__':
    gtk.threads_init()
    app = Mise()
    with gtk.gdk.lock:
        gtk.main()    
