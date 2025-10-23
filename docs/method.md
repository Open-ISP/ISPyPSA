The method description provided here is intended as a relatively plain english
explanation that links the ISPyPSA inputs and model config to the formulation of the
optimisation model in PyPSA.

## Overview

Mix-integer linear programing is used to represent the National Electricity Market's
physical generation and transmission infrastructure, future costs, resource
availability, demand for electricity, policy objectives, and operational and investment
decisions. Optimisation can than be used to determine which combination of operational
and investment decisions would result in the lowest total cost. The methods description
which follows is a plain english description of how various aspects of the NEM are
represented within the mix-integer linear model.

## Nodal representation

Each of the sub regions and REZs is represented as a separate node within the model.
Generators, storage, and loads are located at particular nodes, with energy able to
flow freely, without constraint or losses between any generators, storage, and loads
connected to the same node.

## Transmission

The transmission network is represented as the ability for energy to flow from one node
to another subject to a power constraint. Currently, losses are not implemented with
transmission representation.

### Sub-region to sub-region

Transmission between sub regions is implemented with static constraints on power
flow in each direction. The two values from the ISPyPSA inputs table `flow_paths`,
`forward_direction_mw_summer_typical` and `reverse_direction_mw_summer_typical`, set
the limits on power flow in the forward and reverse flow directions.

TODO: Implement time varying transmission limits making use of the peak demand and
winter provided in the IASR workbook.

### REZ exports

### Custom constraints

## Generation

## Temporal aggregation

##

## Capacity expansion

### Investment periodisation and discounting

## Operational
