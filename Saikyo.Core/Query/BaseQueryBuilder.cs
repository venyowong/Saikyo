using Saikyo.Core.Exceptions;
using Saikyo.Core.Extensions;
using Saikyo.Core.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Saikyo.Core.Query
{
    public abstract class BaseQueryBuilder : IQueryBuilder
    {
        protected List<IQueryBuilder> queryBuilders = new List<IQueryBuilder>();
        private List<Symbol> symbols = new List<Symbol>();
        private QueryType type = QueryType.All;
        protected string key;
        private Symbol ope;
        protected string value;
        protected BaseCollection collection;

        internal BaseQueryBuilder(BaseCollection collection, string key, Symbol ope, string value)
        {
            this.collection = collection;
            this.type = QueryType.Base;
            this.key = key;
            this.ope = ope;
            this.value = value;
        }

        public BaseQueryBuilder(BaseCollection collection, string condition)
        {
            this.collection = collection;
            if (!string.IsNullOrWhiteSpace(condition))
            {
                var key = string.Empty;
                var ope = Symbol.Invalid;
                var value = string.Empty;
                var exp = string.Empty;
                var expType = ExpType.Init;

                #region parse
                for (int i = 0; i < condition.Length; i++)
                {
                    switch (expType)
                    {
                        case ExpType.Init:
                            switch (condition[i])
                            {
                                case '(':
                                    expType = ExpType.Parenthesis;
                                    exp = string.Empty;
                                    continue;
                                case ' ':
                                    continue;
                                default:
                                    expType = ExpType.Key;
                                    exp = condition[i].ToString();
                                    continue;
                            }
                        case ExpType.Parenthesis:
                            switch (condition[i])
                            {
                                case ')':
                                    expType = ExpType.Exp;
                                    this.queryBuilders.Add(this.CreateSubQuery(exp));
                                    exp = string.Empty;
                                    continue;
                                default:
                                    exp = exp + condition[i];
                                    continue;
                            }
                        case ExpType.Key:
                            switch (condition[i])
                            {
                                case ' ':
                                    expType = ExpType.Operator;
                                    key = exp;
                                    exp = string.Empty;
                                    continue;
                                default:
                                    exp = exp + condition[i];
                                    continue;
                            }
                        case ExpType.Exp:
                            switch (condition[i])
                            {
                                case ' ':
                                    continue;
                                case '&':
                                case '|':
                                    if (!string.IsNullOrWhiteSpace(key))
                                    {
                                        this.queryBuilders.Add(this.CreateSubQuery(key, ope, value));
                                        key = string.Empty;
                                        ope = Symbol.Invalid;
                                        value = string.Empty;
                                    }
                                    expType = ExpType.Operator;
                                    exp = condition[i].ToString();
                                    continue;
                                case '(':
                                    expType = ExpType.Parenthesis;
                                    exp = string.Empty;
                                    continue;
                                default:
                                    expType = ExpType.Key;
                                    exp = condition[i].ToString();
                                    continue;
                            }
                        case ExpType.Operator:
                            switch (condition[i])
                            {
                                case ' ':
                                    switch (exp)
                                    {
                                        case ">":
                                            expType = ExpType.Value;
                                            exp = string.Empty;
                                            ope = Symbol.Gt;
                                            continue;
                                        case ">=":
                                            expType = ExpType.Value;
                                            exp = string.Empty;
                                            ope = Symbol.Gte;
                                            continue;
                                        case "<":
                                            expType = ExpType.Value;
                                            exp = string.Empty;
                                            ope = Symbol.Lt;
                                            continue;
                                        case "<=":
                                            expType = ExpType.Value;
                                            exp = string.Empty;
                                            ope = Symbol.Lte;
                                            continue;
                                        case "==":
                                            expType = ExpType.Value;
                                            exp = string.Empty;
                                            ope = Symbol.Eq;
                                            continue;
                                        case "&&":
                                            expType = ExpType.Exp;
                                            exp = string.Empty;
                                            this.symbols.Add(Symbol.And);
                                            continue;
                                        case "||":
                                            expType = ExpType.Exp;
                                            exp = string.Empty;
                                            this.symbols.Add(Symbol.Or);
                                            continue;
                                        default:
                                            throw new ExpressionException(condition, i, "invalid operator, {>, >=, <, <=, ==, &&, ||} are supported");
                                    }
                                default:
                                    exp = exp + condition[i];
                                    continue;
                            }
                        case ExpType.Value:
                            switch (condition[i])
                            {
                                case '\'':
                                    expType = ExpType.SingleQuotes;
                                    exp = string.Empty;
                                    continue;
                                case '"':
                                    expType = ExpType.DoubleQuotes;
                                    exp = string.Empty;
                                    continue;
                                case ' ':
                                    expType = ExpType.Exp;
                                    value = exp;
                                    exp = string.Empty;
                                    continue;
                                default:
                                    exp = exp + condition[i];
                                    continue;
                            }
                        case ExpType.SingleQuotes:
                            switch (condition[i])
                            {
                                case '\\':
                                    if (condition.Length > i + 1 && condition[i + 1] == '\'')
                                    {
                                        exp = exp + '\'';
                                        i++;
                                    }
                                    else
                                    {
                                        exp = exp + condition[i];
                                    }
                                    continue;
                                case '\'':
                                    expType = ExpType.Exp;
                                    value = exp;
                                    exp = string.Empty;
                                    continue;
                                default:
                                    exp = exp + condition[i];
                                    continue;
                            }
                        case ExpType.DoubleQuotes:
                            switch (condition[i])
                            {
                                case '\\':
                                    if (condition.Length > i + 1 && condition[i + 1] == '"')
                                    {
                                        exp = exp + '"';
                                        i++;
                                    }
                                    else
                                    {
                                        exp = exp + condition[i];
                                    }
                                    continue;
                                case '"':
                                    expType = ExpType.Exp;
                                    value = exp;
                                    exp = string.Empty;
                                    continue;
                                default:
                                    exp = exp + condition[i];
                                    continue;
                            }
                    }
                }

                switch (expType)
                {
                    case ExpType.DoubleQuotes:
                    case ExpType.SingleQuotes:
                    case ExpType.Value:
                        this.queryBuilders.Add(this.CreateSubQuery(key, ope, exp));
                        break;
                    case ExpType.Key:
                    case ExpType.Operator:
                        throw new ExpressionException(condition, condition.Length - 1, "Incomplete expression");
                    case ExpType.Parenthesis:
                        throw new ExpressionException(condition, condition.Length - 1, "')' excepted");
                }
                #endregion parse

                if (this.queryBuilders.Any())
                {
                    this.type = QueryType.Compound;
                }
                else
                {
                    if (!string.IsNullOrWhiteSpace(key) && ope != Symbol.Invalid && !string.IsNullOrWhiteSpace(value))
                    {
                        this.type = QueryType.Base;
                        this.key = key;
                        this.ope = ope;
                        this.value = value;
                    }
                }
            }
        }

        public IQueryResult Build()
        {
            if (this.type == QueryType.Base)
            {
                var gather = this.collection.GetGather(this.key);
                if (gather == null)
                {
                    return this.CreateResult();
                }
                if (gather is TextGather)
                {
                    throw new NotSupportedException($"{this.key} is a string with no specified size, and you can't query by it");
                }
                var avl = (IAVLTree)gather;

                var type = this.collection.GetPropertyType(this.key);
                List<Column> values;
                switch (this.ope)
                {
                    case Symbol.Gt:
                        values = avl.Gt(this.value.FromString(type));
                        break;
                    case Symbol.Gte:
                        values = avl.Gte(this.value.FromString(type));
                        break;
                    case Symbol.Lt:
                        values = avl.Lt(this.value.FromString(type));
                        break;
                    case Symbol.Lte:
                        values = avl.Lte(this.value.FromString(type));
                        break;
                    case Symbol.Eq:
                        values = avl.Eq(this.value.FromString(type));
                        break;
                    default:
                        values = new List<Column>();
                        break;
                }
                return this.CreateResult(this.key, values);
            }
            else if (this.type == QueryType.Compound)
            {
                var results = this.queryBuilders.Select(b => b.Build()).ToArray();
                var rlen = results.Length;
                var symbols = this.symbols.ToArray();
                var slen = symbols.Length;

                // and
                for (int i = 0; i < slen; i++)
                {
                    if (symbols[i] != Symbol.And)
                    {
                        continue;
                    }

                    if (i + 1 < rlen)
                    {
                        results[i].And(results[i + 1]);
                        for (int j = i + 1; j < rlen - 1; j++)
                        {
                            results[j] = results[j + 1];
                        }
                        rlen--;
                        for (int j = i + 1; j < slen - 1; j++)
                        {
                            symbols[j] = symbols[j + 1];
                        }
                        slen--;
                        i--;
                    }
                    else
                    {
                        slen = i;
                    }
                }

                // or
                for (int i = slen - 1; i >= 0; i--)
                {
                    if (symbols[i] != Symbol.Or)
                    {
                        throw new SystemException("unexcepted symbol when building, {&&, ||} is valid");
                    }

                    results[i].Or(results[i + 1]);
                }

                return results[0];
            }
            else
            {
                var gather = this.collection.GetGather(this.collection.Key);
                if (gather != null && gather is IAVLTree avl)
                {
                    List<Column> keys = avl.GetAllColumns();
                    return this.CreateResult(this.collection.Key, keys);
                }
                else
                {
                    return this.CreateResult();
                }
            }
        }

        internal abstract IQueryBuilder CreateSubQuery(string key, Symbol ope, string value);

        internal abstract IQueryBuilder CreateSubQuery(string condition);

        internal abstract IQueryResult CreateResult();

        internal abstract IQueryResult CreateResult(string column, List<Column> values);
    }
}
