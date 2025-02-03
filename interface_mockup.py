from ispypsa import io, model, templater, translator

config = io.load_config(path="path/to/config")

#  Read in cache table into a dictionary
iasr_workbook_tables = io.load_iasr_workbook_tables(
    cache_path="path/to/cache", config=config
)

# Create ISPySA template input tables
ispysa_template_input_tables = templater.create_inputs_template(config)
io.write_ispypsa_template(ispysa_template_input_tables, path="path/to/save/template")

# Create PyPSA friendly input tables
ispypsa_inputs = io.read_ispypsa_inputs(path="path/to/save/template")
pysa_friendly_input_tables = translator.create_pypsa_inputs_template(
    config, ispypsa_inputs
)
io.write_pypsa_friendly_inputs(
    pysa_friendly_input_tables, path="path/to/save/pypsa_inputs"
)

# Create PyPSA network
pysa_friendly_input_tables = io.read_pypsa_inputs(path="path/to/save/pypsa_inputs")
pypsa_network = model.build(config, pysa_friendly_input_tables)
io.write_pypsa_network(pypsa_network, path="path/to/save/template")
