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

/**
 * Configuration class for creating a Connection.
 *
 * @since  1.0
 * @author Jonathan H. Wage <jonwage@gmail.com>
 */
class Configuration
{
    /**
     * Array of attributes for this configuration instance.
     *
     * @var array
     */
    protected $attributes = array(
        'riakCmd' => '$',
        'retryConnect' => 0,
        'retryQuery' => 0,
    );

    /**
     * Gets the logger callable.
     *
     * @return callable
     */
    public function getLoggerCallable()
    {
        return isset($this->attributes['loggerCallable']) ? $this->attributes['loggerCallable'] : null;
    }

    /**
     * Set the logger callable.
     *
     * @param callable $loggerCallable
     */
    public function setLoggerCallable($loggerCallable)
    {
        $this->attributes['loggerCallable'] = $loggerCallable;
    }

    /**
     * Get the Riak command prefix.
     *
     * @deprecated 1.1 No longer supported; will be removed for 1.2
     * @return string
     */
    public function getRiakCmd()
    {
        trigger_error('Riak command prefix option is no longer used', E_USER_DEPRECATED);
        return $this->attributes['riakCmd'];
    }

    /**
     * Set the Riak command prefix.
     *
     * @deprecated 1.1 No longer supported; will be removed for 1.2
     * @param string $cmd
     */
    public function setRiakCmd($cmd)
    {
        trigger_error('Riak command prefix option is no longer used', E_USER_DEPRECATED);
        $this->attributes['riakCmd'] = $cmd;
    }

    /**
     * Get the number of times to retry connection attempts after an exception.
     *
     * @return integer
     */
    public function getRetryConnect()
    {
        return $this->attributes['retryConnect'];
    }

    /**
     * Set the number of times to retry connection attempts after an exception.
     *
     * @param boolean|integer $retryConnect
     */
    public function setRetryConnect($retryConnect)
    {
        $this->attributes['retryConnect'] = (integer) $retryConnect;
    }

    /**
     * Get the number of times to retry queries after an exception.
     *
     * @return integer
     */
    public function getRetryQuery()
    {
        return $this->attributes['retryQuery'];
    }

    /**
     * Set the number of times to retry queries after an exception.
     *
     * @param boolean|integer $retryQuery
     */
    public function setRetryQuery($retryQuery)
    {
        $this->attributes['retryQuery'] = (integer) $retryQuery;
    }
}
