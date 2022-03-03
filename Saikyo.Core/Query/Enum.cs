using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Query
{
    internal enum QueryType
    {
        All,
        Base,
        Compound
    }

    internal enum Symbol
    {
        Invalid,
        Gt,
        Gte,
        Lt,
        Lte,
        Eq,
        And,
        Or
    }

    internal enum ExpType
    {
        Init,
        Key,
        Operator,
        Value,
        SingleQuotes,
        DoubleQuotes,
        Parenthesis,
        Exp
    }
}
