import xml.etree.ElementTree as ET
from .Sql_query import SQLQuery


class Bot:
    sqlQuery = SQLQuery()

    def __init__(self):
        self.xml_file = self.sqlQuery.get_bot()
        self.frames = []
        self.model_decision_params = None
        self.model_definition_params = None
        self.number_of_pieces_for_definition = None
        self.number_of_candles = 1000  # config.xml

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Bot, cls).__new__(cls)
        return cls.instance

    def set_params_for_NN(self):
        tree = ET.parse(self.xml_file)
        root = tree.getroot()
        neural_network_params_elem = root.find('neural_network_params')

        model_decision_params = neural_network_params_elem.find("model_decision_params")
        layer_sizes_decision = [int(layer_size.text) for layer_size in model_decision_params.findall("layer_size")]
        activations_decision = [activation.text for activation in model_decision_params.findall("activation")]

        model_definition_params = neural_network_params_elem.find("model_definition_params")
        layer_sizes_definition = [int(layer_size.text) for layer_size in model_definition_params.findall("layer_size")]
        activations_definition = [activation.text for activation in model_definition_params.findall("activation")]

        self.model_decision_params = [layer_sizes_decision, activations_decision]
        self.model_definition_params = [layer_sizes_definition, activations_definition]

        self.number_of_pieces_for_definition = int(self.number_of_candles / self.model_definition_params[0][0])

    def get_params_for_NN(self):
        return self.model_decision_params, self.model_definition_params

    def get_first_layer_size_m_definition(self):
        return self.model_definition_params[0][0]

    def get_number_of_pieces_for_definition(self):
        return self.number_of_pieces_for_definition

    def read_frames_from_xml(self):
        tree = ET.parse(self.xml_file)
        root = tree.getroot()
        frames_elem = root.find('frames')

        for frame in frames_elem.findall('frame'):
            self.frames.append(int(frame.text))

    def get_frames(self):
        return self.frames


