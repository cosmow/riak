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
use Doctrine\Riak\Aggregation\Stage;

/**
 * Fluent interface for adding a $limit stage to an aggregation pipeline.
 *
 * @author alcaeus <alcaeus@alcaeus.org>
 */
class Limit extends Stage
{
    /**
     * @var integer
     */
    private $limit;

    /**
     * @param Builder $builder
     * @param integer $limit
     */
    public function __construct(Builder $builder, $limit)
    {
        parent::__construct($builder);

        $this->limit = (integer) $limit;
    }

    /**
     * {@inheritdoc}
     */
    public function getExpression()
    {
        return array(
            '$limit' => $this->limit
        );
    }
}
