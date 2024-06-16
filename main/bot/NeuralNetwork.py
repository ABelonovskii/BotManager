import tensorflow as tf
import pygad
import pygad.kerasga
import logging


class NeuralNetwork:
    logger = logging.getLogger(__name__)

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(NeuralNetwork, cls).__new__(cls)
        return cls.instance  

    def __init__(self):
        self.model_decision_buy = None
        self.model_decision_sell = None
        self.model_definition_parameters_buy = None
        self.model_definition_parameters_sell = None

    def create_model(self, layer_sizes, activations):
        inputs = tf.keras.layers.Input(layer_sizes[0])
        x = inputs
        for size, activation in zip(layer_sizes[1:], activations[:-1]):
            x = tf.keras.layers.Dense(size, activation=activation)(x)
        outputs = tf.keras.layers.Dense(layer_sizes[-1], activation=activations[-1])(x)
        model = tf.keras.Model(inputs=inputs, outputs=outputs)
        return model

    def create_networks(self, bot):
        model_decision_params, model_definition_params = bot.get_params_for_NN()
        
        self.model_decision_buy = self.create_model(model_decision_params[0], model_decision_params[1])
        self.model_decision_sell = self.create_model(model_decision_params[0], model_decision_params[1])
        self.model_definition_parameters_buy = self.create_model(model_definition_params[0], model_definition_params[1])
        self.model_definition_parameters_sell = self.create_model(model_definition_params[0], model_definition_params[1])

    def print_models_structure(self):
        print("Model decision buy structure:")
        self.model_decision_buy.summary()
        print("\n")
        print("Model decision sell structure:")
        self.model_decision_sell.summary()
        print("\n")
        print("Model definition buy structure:")
        self.model_definition_parameters_buy.summary()
        print("\n")
        print("Model definition sell structure:")
        self.model_definition_parameters_sell.summary()
        print("\n")

    def fill_weights(self, weights):
        start_idx = 0
        end_idx = self.model_decision_buy.count_params()
        decision_buy_weights_matrix = pygad.kerasga.model_weights_as_matrix(
            model=self.model_decision_buy,
            weights_vector=weights[start_idx:end_idx])
    
        start_idx = end_idx
        end_idx = start_idx + self.model_decision_sell.count_params()
        decision_sell_weights_matrix = pygad.kerasga.model_weights_as_matrix(
            model=self.model_decision_sell,
            weights_vector=weights[start_idx:end_idx])
    
        start_idx = end_idx
        end_idx = start_idx + self.model_definition_parameters_buy.count_params()
        definition_parameters_buy_weights_matrix = pygad.kerasga.model_weights_as_matrix(
            model=self.model_definition_parameters_buy,
            weights_vector=weights[start_idx:end_idx])
    
        start_idx = end_idx
        end_idx = start_idx + self.model_definition_parameters_sell.count_params()
        definition_parameters_sell_weights_matrix = pygad.kerasga.model_weights_as_matrix(
            model=self.model_definition_parameters_sell,
            weights_vector=weights[start_idx:end_idx])
    
        self.model_decision_buy.set_weights(weights=decision_buy_weights_matrix)
        self.model_decision_sell.set_weights(weights=decision_sell_weights_matrix)
        self.model_definition_parameters_buy.set_weights(weights=definition_parameters_buy_weights_matrix)
        self.model_definition_parameters_sell.set_weights(weights=definition_parameters_sell_weights_matrix)

    def predict_definition(self, data):
        parameters_buy = self.model_definition_parameters_buy.predict(data)
        parameters_sell = self.model_definition_parameters_sell.predict(data)
        
        return parameters_buy, parameters_sell

    def predict_definition_buy(self, data):
        parameters_buy = self.model_definition_parameters_buy.predict(data)
        return parameters_buy

    def predict_definition_sell(self, data):
        parameters_sell = self.model_definition_parameters_sell.predict(data)
        return parameters_sell

    def predict_decisions_buy(self, signals_buy):
        return self.model_decision_buy.predict(signals_buy)
    
    def predict_decisions_sell(self, signals_sell):
        return self.model_decision_sell.predict(signals_sell)
    
    
    
    
    