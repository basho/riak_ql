-module(riak_ql_ops_flipper).

-export([maybe_flip_op/1]).

maybe_flip_op(less_than_operator)    -> greater_than_operator;
maybe_flip_op(greater_than_operator) -> less_than_operator;
maybe_flip_op(lte)                   -> gte;
maybe_flip_op(gte)                   -> lte;
maybe_flip_op(Op)                    -> Op.
