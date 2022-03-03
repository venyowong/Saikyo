using System;
using System.Collections.Generic;
using System.Text;

namespace Saikyo.Core.Exceptions
{
    public class ExpressionException : Exception
    {
        private string expression;
        private int position;
        private string message;

        public ExpressionException(string exp, int pos, string msg)
            : base(msg)
        {
            this.expression = exp;
            this.position = pos;
            this.message = msg;
        }
    }
}
