# Author: Aditya Arora
# 23, May, 2016

using Mocha

#HyperParamters
n_hidden_layer		= 3
hidden_layer_sizes	= [100,150,200]
neuron				= Neurons.Sigmoid()
corruption_rates	= [0.1,0.2,0.3]
param_keys			= ["ip-layer-$i" for i = 1:n_hidden_layer]
batch_size			= 50
data size			= 1000
epochs				= 15
reg_parameter		= 0
momentum			= 0.0
step_size			= 0.001

#Initialisation
srand(12345678)
backend = DefaultBackend()
init(backend)

#Describe Layers
data_layer = HDF5DataLayer(name="train-data", source="data/train.txt",batch_size=batch_size, shuffle=true)

#data_layer is zero-hot not one-hot preprocessed (explained in documentation)
rename_layer = IdentityLayer(bottoms=[:data], tops=[:ip0])

#for ease of iteration we rename data layer to ip0
hidden_layers = [InnerProductLayer(name="ip-$i", param_key=param_keys[i], output_dim=hidden_layer_sizes[i],
	neuron=neuron, bottoms=[symbol("ip$(i-1)")], tops=[symbol("ip$i")]) for i = 1:n_hidden_layer]
	#the layers used to encode the data

#The Autoencoder
for i = 1:n_hidden_layer
	#Describe autoencoder layers for the particular hidden layer
	ae_data_layer = SplitLayer(bottoms=[symbol("ip$(i-1)")], tops=[:orig_data, :corrupt_data])
		#Duplicates the previous layer
	corrupt_layer = RandomMaskLayer(ratio=corruption_rates[i], bottoms=[:corrupt_data])
		#Corrupts one copy of the previous layer
	encode_layer= copy(hidden_layers[i], bottoms=[:corrupt_data])
		#The layer that encodes the data to its vector space representation
	recon_layer = TiedInnerProductLayer(name="tied-ip-$i", tied_param_key=param_keys[i], tops=[:recon],
		bottoms=[symbol("ip$i")])
		#The layer that reconstructs the vector space representation
	recon_loss_layer = SquareLossLayer(bottoms=[:recon, :orig_data])
		#The layer that computes square loss error

	#Describe the neural network
	da_layers = [data_layer, rename_layer, ae_data_layer, corrupt_layer, hidden_layers[1:i-1]...,
		encode_layer, recon_layer, recon_loss_layer]
	da = Net("Denoising-Autoencoder-$i", backend, da_layers)
	println(da)

	#Freeze all but the layers being learned
	freeze_all!(da)
	unfreeze!(da, "ip-$i", "tied-ip-$i")

	base_dir = "pretrain-$i"
	method = SGD()
	pretrain_params= make_solver_parameters(method, max_iter=div(epochs*data_size,batch_size),
		regu_coef=reg_parameter, mom_policy=MomPolicy.Fixed(momentum),
		lr_policy=LRPolicy.Fixed(step_size), load_from=base_dir)
	solver = Solver(method, pretrain_params)
	add_coffee_break(solver, TrainingSummary(), every_n_iter=500)
	add_coffee_break(solver, Snapshot(base_dir), every_n_iter=800)
	#Save model to directory so that it can be reloaded next iteration where the net layer is trained
	solve(solver, da)
	destroy(da)
end

shutdown(backend)
