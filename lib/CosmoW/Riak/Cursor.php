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

namespace CosmoW\Riak;

use Doctrine\Riak\Util\ReadPreference;

/**
 * Wrapper for the PHP RiakCursor class.
 *
 * @since  1.0
 * @author Jonathan H. Wage <jonwage@gmail.com>
 */
class Cursor implements CursorInterface
{
    /**
     * The Collection instance used for recreating this cursor.
     *
     * This is also used to access the Connection for reinitializing during
     * retry attempts.
     *
     * @var Collection
     */
    protected $collection;

    /**
     * The RiakCursor instance being wrapped.
     *
     * @var \RiakCursor
     */
    protected $riakCursor;

    /**
     * Number of times to retry queries.
     *
     * @var integer
     */
    protected $numRetries;

    /**
     * Whether to use the document's "_id" value as its iteration key.
     *
     * If false, the position of the document in the result set will be reported
     * instead. This is useful for documents that have non-scalar IDs.
     *
     * @var boolean
     */
    protected $useIdentifierKeys = true;

    protected $query = array();
    protected $fields = array();
    protected $hint;
    protected $immortal;
    protected $options = array();
    protected $batchSize;
    protected $limit;
    protected $readPreference;
    protected $readPreferenceTags;
    protected $skip;
    protected $slaveOkay;
    protected $snapshot;
    protected $sort;
    protected $tailable;
    protected $timeout;

    /**
     * Constructor.
     *
     * The wrapped RiakCursor instance may change if the cursor is recreated.
     *
     * @param Collection   $collection  Collection used to create this Cursor
     * @param \RiakCursor $riakCursor RiakCursor instance being wrapped
     * @param array        $query       Query criteria
     * @param array        $fields      Selected fields (projection)
     * @param integer      $numRetries  Number of times to retry queries
     */
    public function __construct(Collection $collection, \RiakCursor $riakCursor, array $query = array(), array $fields = array(), $numRetries = 0)
    {
        $this->collection = $collection;
        $this->riakCursor = $riakCursor;
        $this->query = $query;
        $this->fields = $fields;
        $this->numRetries = (integer) $numRetries;
    }

    /**
     * Wrapper method for RiakCursor::addOption().
     *
     * @see http://php.net/manual/en/mongocursor.addoption.php
     * @param string $key
     * @param mixed $value
     * @return self
     */
    public function addOption($key, $value)
    {
        $this->options[$key] = $value;
        $this->riakCursor->addOption($key, $value);
        return $this;
    }

    /**
     * Wrapper method for RiakCursor::batchSize().
     *
     * @see http://php.net/manual/en/mongocursor.batchsize.php
     * @param integer $num
     * @return self
     */
    public function batchSize($num)
    {
        $num = (integer) $num;
        $this->batchSize = $num;
        $this->riakCursor->batchSize($num);
        return $this;
    }

    /**
     * Wrapper method for RiakCursor::count().
     *
     * @see http://php.net/manual/en/countable.count.php
     * @see http://php.net/manual/en/mongocursor.count.php
     * @param boolean $foundOnly
     * @return integer
     */
    public function count($foundOnly = false)
    {
        $cursor = $this;
        return $this->retry(function() use ($cursor, $foundOnly) {
            return $cursor->getRiakCursor()->count($foundOnly);
        }, true);
    }

    /**
     * Wrapper method for RiakCursor::current().
     *
     * @see http://php.net/manual/en/iterator.current.php
     * @see http://php.net/manual/en/mongocursor.current.php
     * @return array|null
     */
    public function current()
    {
        $current = $this->riakCursor->current();
        if ($current instanceof \RiakGridFSFile) {
            $document = $current->file;
            $document['file'] = new GridFSFile($current);
            $current = $document;
        }
        return $current;
    }

    /**
     * Wrapper method for RiakCursor::dead().
     *
     * @see http://php.net/manual/en/mongocursor.dead.php
     * @return boolean
     */
    public function dead()
    {
        return $this->riakCursor->dead();
    }

    /**
     * Wrapper method for RiakCursor::explain().
     *
     * @see http://php.net/manual/en/mongocursor.explain.php
     * @return array
     */
    public function explain()
    {
        $cursor = $this;
        return $this->retry(function() use ($cursor) {
            return $cursor->getRiakCursor()->explain();
        }, true);
    }

    /**
     * Wrapper method for RiakCursor::fields().
     *
     * @param array $f Fields to return (or not return).
     *
     * @see http://php.net/manual/en/mongocursor.fields.php
     * @return self
     */
    public function fields(array $f)
    {
        $this->fields = $f;
        $this->riakCursor->fields($f);
        return $this;
    }

    /**
     * Return the collection for this cursor.
     *
     * @return Collection
     */
    public function getCollection()
    {
        return $this->collection;
    }

    /**
     * Return the connection for this cursor.
     *
     * @deprecated 1.1 Will be removed for 2.0
     * @return Connection
     */
    public function getConnection()
    {
        return $this->collection->getDatabase()->getConnection();
    }

    /**
     * Return the selected fields (projection).
     *
     * @return array
     */
    public function getFields()
    {
        return $this->fields;
    }

    /**
     * Returns the RiakCursor instance being wrapped.
     *
     * @return \RiakCursor
     */
    public function getRiakCursor()
    {
        return $this->riakCursor;
    }

    /**
     * Wrapper method for RiakCursor::getNext().
     *
     * @see http://php.net/manual/en/mongocursor.getnext.php
     * @return array|null
     */
    public function getNext()
    {
        $cursor = $this;
        $next = $this->retry(function() use ($cursor) {
            return $cursor->getRiakCursor()->getNext();
        }, false);
        if ($next instanceof \RiakGridFSFile) {
            $document = $next->file;
            $document['file'] = new GridFSFile($next);
            $next = $document;
        }
        return $next;
    }

    /**
     * Return the query criteria.
     *
     * @return array
     */
    public function getQuery()
    {
        return $this->query;
    }

    /**
     * Wrapper method for RiakCursor::getReadPreference().
     *
     * @see http://php.net/manual/en/mongocursor.getreadpreference.php
     * @return array
     */
    public function getReadPreference()
    {
        return $this->riakCursor->getReadPreference();
    }

    /**
     * Set the read preference.
     *
     * @see http://php.net/manual/en/mongocursor.setreadpreference.php
     * @param string $readPreference
     * @param array  $tags
     * @return self
     */
    public function setReadPreference($readPreference, array $tags = null)
    {
        if ($tags !== null) {
            $this->riakCursor->setReadPreference($readPreference, $tags);
        } else {
            $this->riakCursor->setReadPreference($readPreference);
        }

        $this->readPreference = $readPreference;
        $this->readPreferenceTags = $tags;

        return $this;
    }

    /**
     * Reset the cursor and return its first result.
     *
     * The cursor will be reset both before and after the single result is
     * fetched. The original cursor limit (if any) will remain in place.
     *
     * @see Iterator::getSingleResult()
     * @return array|object|null
     */
    public function getSingleResult()
    {
        $originalLimit = $this->limit;
        $originalUseIdentifierKeys = $this->useIdentifierKeys;

        $this->reset();
        $this->limit(1);
        $this->setUseIdentifierKeys(false);

        $result = current($this->toArray()) ?: null;

        $this->reset();
        $this->limit($originalLimit);
        $this->setUseIdentifierKeys($originalUseIdentifierKeys);

        return $result;
    }

    /**
     * Return whether the document's "_id" value is used as its iteration key.
     *
     * @since 1.2
     * @return boolean
     */
    public function getUseIdentifierKeys()
    {
        return $this->useIdentifierKeys;
    }

    /**
     * Set whether to use the document's "_id" value as its iteration key.
     *
     * @since 1.2
     * @param boolean $useIdentifierKeys
     * @return self
     */
    public function setUseIdentifierKeys($useIdentifierKeys)
    {
        $this->useIdentifierKeys = (boolean) $useIdentifierKeys;

        return $this;
    }

    /**
     * Wrapper method for RiakCursor::hasNext().
     *
     * @see http://php.net/manual/en/mongocursor.hasnext.php
     * @return boolean
     */
    public function hasNext()
    {
        $cursor = $this;
        return $this->retry(function() use ($cursor) {
            return $cursor->getRiakCursor()->hasNext();
        }, false);
    }

    /**
     * Wrapper method for RiakCursor::hint().
     *
     * @see http://php.net/manual/en/mongocursor.hint.php
     * @param array|string $keyPattern
     * @return self
     */
    public function hint($keyPattern)
    {
        $this->hint = $keyPattern;
        $this->riakCursor->hint($keyPattern);
        return $this;
    }

    /**
     * Wrapper method for RiakCursor::immortal().
     *
     * @see http://php.net/manual/en/mongocursor.immortal.php
     * @param boolean $liveForever
     * @return self
     */
    public function immortal($liveForever = true)
    {
        $liveForever = (boolean) $liveForever;
        $this->immortal = $liveForever;
        $this->riakCursor->immortal($liveForever);
        return $this;
    }

    /**
     * Wrapper method for RiakCursor::info().
     *
     * @see http://php.net/manual/en/mongocursor.info.php
     * @return array
     */
    public function info()
    {
        return $this->riakCursor->info();
    }

    /**
     * Wrapper method for RiakCursor::key().
     *
     * @see http://php.net/manual/en/iterator.key.php
     * @see http://php.net/manual/en/mongocursor.key.php
     * @return mixed
     */
    public function key()
    {
        // TODO: Track position internally to avoid repeated info() calls
        if ( ! $this->useIdentifierKeys) {
            $info = $this->riakCursor->info();

            return isset($info['at']) ? $info['at'] : null;
        }

        return $this->riakCursor->key();
    }

    /**
     * Wrapper method for RiakCursor::limit().
     *
     * @see http://php.net/manual/en/mongocursor.limit.php
     * @param integer $num
     * @return self
     */
    public function limit($num)
    {
        $num = (integer) $num;
        $this->limit = $num;
        $this->riakCursor->limit($num);
        return $this;
    }

    /**
     * Wrapper method for RiakCursor::next().
     *
     * @see http://php.net/manual/en/iterator.next.php
     * @see http://php.net/manual/en/mongocursor.next.php
     */
    public function next()
    {
        $cursor = $this;
        $this->retry(function() use ($cursor) {
            $cursor->getRiakCursor()->next();
        }, false);
    }

    /**
     * Recreates the internal RiakCursor.
     */
    public function recreate()
    {
        $this->riakCursor = $this->collection->getRiakCollection()->find($this->query, $this->fields);
        if ($this->hint !== null) {
            $this->riakCursor->hint($this->hint);
        }
        if ($this->immortal !== null) {
            $this->riakCursor->immortal($this->immortal);
        }
        foreach ($this->options as $key => $value) {
            $this->riakCursor->addOption($key, $value);
        }
        if ($this->batchSize !== null) {
            $this->riakCursor->batchSize($this->batchSize);
        }
        if ($this->limit !== null) {
            $this->riakCursor->limit($this->limit);
        }
        if ($this->skip !== null) {
            $this->riakCursor->skip($this->skip);
        }
        if ($this->slaveOkay !== null) {
            $this->setRiakCursorSlaveOkay($this->slaveOkay);
        }
        // Set read preferences after slaveOkay, since they may be more specific
        if ($this->readPreference !== null) {
            if ($this->readPreferenceTags !== null) {
                $this->riakCursor->setReadPreference($this->readPreference, $this->readPreferenceTags);
            } else {
                $this->riakCursor->setReadPreference($this->readPreference);
            }
        }
        if ($this->snapshot) {
            $this->riakCursor->snapshot();
        }
        if ($this->sort !== null) {
            $this->riakCursor->sort($this->sort);
        }
        if ($this->tailable !== null) {
            $this->riakCursor->tailable($this->tailable);
        }
        if ($this->timeout !== null) {
            $this->riakCursor->timeout($this->timeout);
        }
    }

    /**
     * Wrapper method for RiakCursor::reset().
     *
     * @see http://php.net/manual/en/iterator.reset.php
     * @see http://php.net/manual/en/mongocursor.reset.php
     */
    public function reset()
    {
        $this->riakCursor->reset();
    }

    /**
     * Wrapper method for RiakCursor::rewind().
     *
     * @see http://php.net/manual/en/iterator.rewind.php
     * @see http://php.net/manual/en/mongocursor.rewind.php
     */
    public function rewind()
    {
        $cursor = $this;
        $this->retry(function() use ($cursor) {
            $cursor->getRiakCursor()->rewind();
        }, false);
    }

    /**
     * Set whether secondary read queries are allowed for this cursor.
     *
     * This method wraps setSlaveOkay() for driver versions before 1.3.0. For
     * newer drivers, this method either wraps setReadPreference() method and
     * specifies SECONDARY_PREFERRED or does nothing, depending on whether
     * setReadPreference() exists.
     *
     * @param boolean $ok
     */
    public function setRiakCursorSlaveOkay($ok)
    {
        if (version_compare(phpversion('mongo'), '1.3.0', '<')) {
            $this->riakCursor->slaveOkay($ok);
            return;
        }

        /* RiakCursor::setReadPreference() may not exist until 1.4.0. Although
         * we could throw an exception here, it's more user-friendly to NOP.
         */
        if (!method_exists($this->riakCursor, 'setReadPreference')) {
            return;
        }

        if ($ok) {
            // Preserve existing tags for non-primary read preferences
            $readPref = $this->riakCursor->getReadPreference();
            $tags = !empty($readPref['tagsets']) ? ReadPreference::convertTagSets($readPref['tagsets']) : array();
            $this->riakCursor->setReadPreference(\RiakClient::RP_SECONDARY_PREFERRED, $tags);
        } else {
            $this->riakCursor->setReadPreference(\RiakClient::RP_PRIMARY);
        }
    }

    /**
     * Wrapper method for RiakCursor::skip().
     *
     * @see http://php.net/manual/en/mongocursor.skip.php
     * @param integer $num
     * @return self
     */
    public function skip($num)
    {
        $num = (integer) $num;
        $this->skip = $num;
        $this->riakCursor->skip($num);
        return $this;
    }

    /**
     * Wrapper method for RiakCursor::slaveOkay().
     *
     * @see http://php.net/manual/en/mongocursor.slaveokay.php
     * @param boolean $ok
     * @return self
     */
    public function slaveOkay($ok = true)
    {
        $ok = (boolean) $ok;
        $this->slaveOkay = $ok;
        $this->setRiakCursorSlaveOkay($ok);
        return $this;
    }

    /**
     * Wrapper method for RiakCursor::snapshot().
     *
     * @see http://php.net/manual/en/mongocursor.snapshot.php
     * @return self
     */
    public function snapshot()
    {
        $this->snapshot = true;
        $this->riakCursor->snapshot();
        return $this;
    }

    /**
     * Wrapper method for RiakCursor::sort().
     *
     * @see http://php.net/manual/en/mongocursor.sort.php
     * @param array $fields
     * @return self
     */
    public function sort($fields)
    {
        foreach ($fields as $fieldName => $order) {
            if (is_string($order)) {
                $order = strtolower($order) === 'asc' ? 1 : -1;
            }

            if (is_scalar($order)) {
                $fields[$fieldName] = (integer) $order;
            }
        }
        $this->sort = $fields;
        $this->riakCursor->sort($fields);
        return $this;
    }

    /**
     * Wrapper method for RiakCursor::tailable().
     *
     * @see http://php.net/manual/en/mongocursor.tailable.php
     * @param boolean $tail
     * @return self
     */
    public function tailable($tail = true)
    {
        $tail = (boolean) $tail;
        $this->tailable = $tail;
        $this->riakCursor->tailable($tail);
        return $this;
    }

    /**
     * Wrapper method for RiakCursor::timeout().
     *
     * @see http://php.net/manual/en/mongocursor.timeout.php
     * @param integer $ms
     * @return self
     */
    public function timeout($ms)
    {
        $this->timeout = (integer) $ms;
        $this->riakCursor->timeout($ms);
        return $this;
    }

    /**
     * Return the cursor's results as an array.
     *
     * @see Iterator::toArray()
     * @param boolean $useIdentifierKeys Deprecated since 1.2; will be removed in 2.0
     * @return array
     */
    public function toArray($useIdentifierKeys = null)
    {
        $originalUseIdentifierKeys = $this->useIdentifierKeys;
        $useIdentifierKeys = isset($useIdentifierKeys) ? (boolean) $useIdentifierKeys : $this->useIdentifierKeys;
        $cursor = $this;

        /* Let iterator_to_array() decide to use keys or not. This will avoid
         * superfluous RiakCursor::info() from the key() method until the
         * cursor position is tracked internally.
         */
        $this->useIdentifierKeys = true;

        $results = $this->retry(function() use ($cursor, $useIdentifierKeys) {
            return iterator_to_array($cursor, $useIdentifierKeys);
        }, true);

        $this->useIdentifierKeys = $originalUseIdentifierKeys;

        return $results;
    }

    /**
     * Wrapper method for RiakCursor::valid().
     *
     * @see http://php.net/manual/en/iterator.valid.php
     * @see http://php.net/manual/en/mongocursor.valid.php
     * @return boolean
     */
    public function valid()
    {
        return $this->riakCursor->valid();
    }

    /**
     * Conditionally retry a closure if it yields an exception.
     *
     * If the closure does not return successfully within the configured number
     * of retries, its first exception will be thrown.
     *
     * The $recreate parameter may be used to recreate the RiakCursor between
     * retry attempts.
     *
     * @param \Closure $retry
     * @param boolean $recreate
     * @return mixed
     */
    protected function retry(\Closure $retry, $recreate = false)
    {
        if ($this->numRetries < 1) {
            return $retry();
        }

        $firstException = null;

        for ($i = 0; $i <= $this->numRetries; $i++) {
            try {
                return $retry();
            } catch (\RiakCursorException $e) {
            } catch (\RiakConnectionException $e) {
            }

            if ($firstException === null) {
                $firstException = $e;
            }
            if ($i === $this->numRetries) {
                throw $firstException;
            }
            if ($recreate) {
                $this->recreate();
            }
        }
    }
}
