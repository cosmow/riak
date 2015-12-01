<?php
/*
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * and is licensed under the MIT license. For more information, see
 * <http://www.doctrine-project.org>.
 */

namespace Doctrine\Riak\Aggregation\Stage;

use Doctrine\Riak\Aggregation\Builder;
use Doctrine\Riak\Aggregation\Expr;
use Doctrine\Riak\Aggregation\Stage;

/**
 * Fluent interface for adding a $group stage to an aggregation pipeline.
 *
 * @author alcaeus <alcaeus@alcaeus.org>
 */
class Group extends Stage
{
    /**
     * @var Expr
     */
    protected $expr;

    /**
     * {@inheritdoc}
     */
    public function __construct(Builder $builder)
    {
        $this->expr = new Expr();

        parent::__construct($builder);
    }

    /**
     * {@inheritdoc}
     */
    public function getExpression()
    {
        return array(
            '$group' => $this->expr->getExpression()
        );
    }

    /**
     * Returns an array of all unique values that results from applying an
     * expression to each document in a group of documents that share the same
     * group by key. Order of the elements in the output array is unspecified.
     *
     * AddToSet is an accumulator operation only available in the group stage.
     *
     * @see Expr::addToSet
     * @param mixed|Expr $expression
     * @return Operator
     */
    public function addToSet($expression)
    {
        $this->expr->addToSet($expression);

        return $this;
    }

    /**
     * Returns the average value of the numeric values that result from applying
     * a specified expression to each document in a group of documents that
     * share the same group by key. Ignores nun-numeric values.
     *
     * @see Expr::avg
     * @param mixed|Expr $expression
     * @return Operator
     */
    public function avg($expression)
    {
        $this->expr->avg($expression);

        return $this;
    }

    /**
     * Used to use an expression as field value. Can be any expression
     *
     * @see Expr::expression
     * @param mixed|Expr $value
     * @return self
     */
    public function expression($value)
    {
        $this->expr->expression($value);

        return $this;
    }

    /**
     * Set the current field for building the expression.
     *
     * @see Expr::field
     * @param string $fieldName
     * @return self
     */
    public function field($fieldName)
    {
        $this->expr->field($fieldName);

        return $this;
    }

    /**
     * Returns the value that results from applying an expression to the first
     * document in a group of documents that share the same group by key. Only
     * meaningful when documents are in a defined order.
     *
     * @see Expr::first
     * @param mixed|Expr $expression
     * @return Operator
     */
    public function first($expression)
    {
        $this->expr->first($expression);

        return $this;
    }

    /**
     * Returns the value that results from applying an expression to the last
     * document in a group of documents that share the same group by a field.
     * Only meaningful when documents are in a defined order.
     *
     * @see Expr::last
     * @param mixed|Expr $expression
     * @return Operator
     */
    public function last($expression)
    {
        $this->expr->last($expression);

        return $this;
    }

    /**
     * Returns the highest value that results from applying an expression to
     * each document in a group of documents that share the same group by key.
     *
     * @see Expr::max
     * @param mixed|Expr $expression
     * @return Operator
     */
    public function max($expression)
    {
        $this->expr->max($expression);

        return $this;
    }

    /**
     * Returns the lowest value that results from applying an expression to each
     * document in a group of documents that share the same group by key.
     *
     * @see Expr::min
     * @param mixed|Expr $expression
     * @return Operator
     */
    public function min($expression)
    {
        $this->expr->min($expression);

        return $this;
    }

    /**
     * Returns an array of all values that result from applying an expression to
     * each document in a group of documents that share the same group by key.
     *
     * @see Expr::push
     * @param mixed|Expr $expression
     * @return Operator
     */
    public function push($expression)
    {
        $this->expr->push($expression);

        return $this;
    }

    /**
     * Calculates and returns the sum of all the numeric values that result from
     * applying a specified expression to each document in a group of documents
     * that share the same group by key. Ignores nun-numeric values.
     *
     * @return Operator
     */
    public function sum($expression)
    {
        $this->expr->sum($expression);

        return $this;
    }
}
