import random
import time
from functools import partial

import numpy as np
import ray
import ray.rllib.algorithms.qmix.qmix as qmix
# import ray.rllib.agents.trainer_template as tt
import ray.rllib.env.multi_agent_env
import ray.rllib.models as models
import world_of_supply_rllib as wsr
from gymnasium.spaces import Box, Discrete, MultiDiscrete, Tuple
from ray.rllib.algorithms.ppo import PPO, PPOConfig
# from ray.rllib.agents.ppo.ppo_torch_policy import PPOTorchPolicy
# from ray.rllib.agents.qmix.qmix_policy import QMixTorchPolicy
from ray.rllib.algorithms.ppo.ppo_tf_policy import PPOTF2Policy as PPOTFPolicy
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_action_dist import MultiCategorical
from ray.rllib.utils import try_import_tf
from ray.tune.logger import pretty_print
from world_of_supply_rllib import Utils
from world_of_supply_rllib_models import FacilityNet

tf1_module, tf, version = try_import_tf()

ray.shutdown()
ray.init()

# Configuration ===============================================================================

env_config = {
    'episod_duration': 1000,
    'global_reward_weight_producer': 0.90,
    'global_reward_weight_consumer': 0.90,
    'downsampling_rate': 20
}
env = wsr.WorldOfSupplyEnv(env_config)

base_trainer_config = {
    'env_config': env_config,
    # 'timesteps_per_iteration': 25000,

    # == Environment Settings ==
    #'lr': 0.0005,
    # 'gamma': 0.99,

    # === Settings for the Trainer process ===
    # 'train_batch_size': 2000,
    # 'batch_mode': 'complete_episodes',
    # 'rollout_fragment_length': 50,
}

ppo_policy_config_producer = {
    "model": {
        "fcnet_hiddens": [128, 128],

        #"custom_model": "facility_net"
    }
}

ppo_policy_config_consumer = {
    "model": {
        "fcnet_hiddens": [256, 256],

        #"custom_model": "facility_net",

        # == LSTM ==
        #"use_lstm": True,
        #"max_seq_len": 8,
        #"lstm_cell_size": 128,
        #"lstm_use_prev_action_reward": False,
    }
}


# Model Configuration ===============================================================================

models.ModelCatalog.register_custom_model("facility_net", FacilityNet)

def print_model_summaries():
    config = models.MODEL_DEFAULTS.copy()
    config.update({"custom_model": "facility_net"})
    facility_net = models.ModelCatalog.get_model_v2(
        obs_space = env.observation_space,
        action_space = env.action_space['consumer'],
        num_outputs = 1,
        model_config = config)
    facility_net.rnn_model.summary()


# Policy Configuration ===============================================================================

policies = {
        'baseline_producer': (wsr.ProducerSimplePolicy, env.observation_space, env.action_space['producer'], wsr.SimplePolicy.get_config_from_env(env)),
        'baseline_consumer': (wsr.ConsumerSimplePolicy, env.observation_space, env.action_space['consumer'], wsr.SimplePolicy.get_config_from_env(env)),
        'ppo_producer': (PPOTFPolicy, env.observation_space, env.action_space['producer'], ppo_policy_config_producer),
        'ppo_consumer': (PPOTFPolicy, env.observation_space, env.action_space['consumer'], ppo_policy_config_consumer)
    }

def filter_keys(d, keys):
    return {k:v for k,v in d.items() if k in keys}

policy_mapping_global = {
        'SteelFactoryCell_1p': 'baseline_producer',
        'SteelFactoryCell_1c': 'baseline_consumer',
        'LumberFactoryCell_2p': 'baseline_producer',
        'LumberFactoryCell_2c': 'baseline_consumer',
        'ToyFactoryCell_3p': 'ppo_producer',
        'ToyFactoryCell_3c': 'ppo_consumer',
        'ToyFactoryCell_4p': 'ppo_producer',
        'ToyFactoryCell_4c': 'ppo_consumer',
        'ToyFactoryCell_5p': 'ppo_producer',
        'ToyFactoryCell_5c': 'ppo_consumer',
        'WarehouseCell_6p': 'baseline_producer',
        'WarehouseCell_6c': 'baseline_consumer',
        'WarehouseCell_7p': 'baseline_producer',
        'WarehouseCell_7c': 'baseline_consumer',
        'RetailerCell_8p': 'baseline_producer',
        'RetailerCell_8c': 'baseline_consumer',
        'RetailerCell_9p': 'baseline_producer',
        'RetailerCell_9c': 'baseline_consumer',
    }

def update_policy_map(policy_map, i = 0, n_iterations = 0): # apply all changes by default
    pass
#    if i == int(n_iterations/100*25):
#        policy_map['WarehouseCell_6p'] = 'ppo_producer'
#        policy_map['WarehouseCell_6c'] = 'ppo_consumer'

#    if i == int(n_iterations/100*35):
#        policy_map['WarehouseCell_7p'] = 'ppo_producer'
#        policy_map['WarehouseCell_7c'] = 'ppo_consumer'


def create_policy_mapping_fn(policy_map):
    # policy mapping is sampled once per episod
    def mapping_fn(agent_id):
        for f_filter, policy_name in policy_map.items():
            if f_filter in agent_id:
                return policy_name

    return mapping_fn



# Training Routines ===============================================================================

def print_training_results(result):
    keys = ['date', 'episode_len_mean', 'episodes_total', 'episode_reward_max', 'episode_reward_mean', 'episode_reward_min',
            'timesteps_total', 'policy_reward_max', 'policy_reward_mean', 'policy_reward_min']
    for k in keys:
        print(f"- {k}: {result[k]}")

def play_baseline(n_iterations):

    HandCodedTrainer = tt.build_trainer("HandCoded", wsr.SimplePolicy)
    ext_conf = {
            "multiagent": {
                "policies": filter_keys(policies, ['baseline_producer', 'baseline_consumer']),
                "policy_mapping_fn": lambda agent_id: 'baseline_producer' if Utils.is_producer_agent(agent_id) else 'baseline_consumer',
                "policies_to_train": ['baseline_producer', 'baseline_consumer']
            }
        }
    handcoded_trainer = HandCodedTrainer(
        env = wsr.WorldOfSupplyEnv,
        config = dict(base_trainer_config, **ext_conf))

    for i in range(n_iterations):
        print("== Iteration", i, "==")
        print_training_results(handcoded_trainer.train())

    return handcoded_trainer


def train_ppo(n_iterations):

    policy_map = policy_mapping_global.copy()

    ppo_trainer = (
        PPOConfig()
        .training(
            model={"vf_share_layers": True,},
            vf_clip_param=200.0,
            vf_loss_coeff= 20.00,
            lr=2e-4,
            gamma=0.99,
            train_batch_size=2000
        )
        .rollouts(
            num_rollout_workers=16,
            rollout_fragment_length=125,
            batch_mode='complete_episodes'
        )
        .reporting(
            min_train_timesteps_per_iteration=25000
        )
        .resources(num_gpus=1)
        .multi_agent(
            policies=filter_keys(policies, set(policy_mapping_global.values())),
            policy_mapping_fn=lambda agent_id: policy_mapping_global[agent_id],
            policies_to_train=['ppo_producer', 'ppo_consumer']
        )
        .framework(
            framework='tf2'
        )
        .environment(
            env=wsr.WorldOfSupplyEnv,
            env_config=env_config,
            disable_env_checking=True
        )
        .build()
    )

    print(f"Environment: action space producer {env.action_space['producer']}, action space consumer {env.action_space['consumer']}, observation space {env.observation_space}")

    training_start_time = time.process_time()
    for i in range(n_iterations):
        print(f"\n== Iteration {i} ==")
        update_policy_map(policy_map, i, n_iterations)
        print(f"- policy map: {policy_map}")

        ppo_trainer.workers.foreach_worker(
            lambda ev: ev.foreach_env(
                lambda env: env.set_iteration(i, n_iterations)))

        t = time.process_time()
        result = ppo_trainer.train()
        print(f"Iteration {i} took [{(time.process_time() - t):.2f}] seconds")
        print_training_results(result)
        print(f"Training ETA: [{(time.process_time() - training_start_time)*(n_iterations/(i+1)-1)/60/60:.2f}] hours to go")

    return ppo_trainer
