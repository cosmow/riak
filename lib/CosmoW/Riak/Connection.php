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

use Doctrine\Common\EventManager;
use Doctrine\Riak\Event\EventArgs;
use Doctrine\Riak\Util\ReadPreference;
use CosmoW\ODM\Riak\RiakException;
use Riak;
use Riak\Client as RiakClient;
use Riak\Node;

/**
 * Wrapper for the RiakClient class.
 *
 * @since  1.0
 * @author Jonathan H. Wage <jonwage@gmail.com>
 */
class Connection
{
    /**
     * The PHP RiakClient instance being wrapped.
     *
     * @var RiakClient
     */
    protected $riakClient;

    /**
     * Server string used to construct the RiakClient instance (optional).
     *
     * @var string
     */
    protected $server;

    /**
     * Options used to construct the RiakClient instance (optional).
     *
     * @var array
     */
    protected $options = array();

    /**
     * The Configuration for this connection.
     *
     * @var Configuration
     */
    protected $config;

    /**
     * The EventManager used to dispatch events.
     *
     * @var \Doctrine\Common\EventManager
     */
    protected $eventManager;

    /**
     * Constructor.
     *
     * If $server is an existing RiakClient instance, the $options parameter
     * will not be used.
     *
     * @param string|RiakClient $server  Server string or RiakClient instance
     * @param array               $options RiakClient constructor options
     * @param Configuration       $config  Configuration instance
     * @param EventManager        $evm     EventManager instance
     */
    public function __construct($server = null, array $options = array(), Configuration $config = null, EventManager $evm = null)
    {
        if ($server instanceof RiakClient) {
            $this->riakClient = $server;
        } else {
            $this->server = $server;
            $this->options = $options;
        }
        $this->config = $config ? $config : new Configuration();
        $this->eventManager = $evm ? $evm : new EventManager();
    }

    /**
     * Wrapper method for RiakClient::close().
     *
     * @see http://php.net/manual/en/mongoclient.close.php
     * @return boolean
     */
    public function close()
    {
        $this->initialize();
        return $this->riakClient->close();
    }

    /**
     * Wrapper method for RiakClient::connect().
     *
     * @see http://php.net/manual/en/mongoclient.connect.php
     * @return boolean
     */
    public function connect()
    {
        $this->initialize();

        $riakClient = $this->riakClient;
        return $this->retry(function() use ($riakClient) {
            return $riakClient->connect();
        });
    }

    /**
     * Wrapper method for RiakClient::dropDB().
     *
     * This method will dispatch preDropDatabase and postDropDatabase events.
     *
     * @see http://php.net/manual/en/mongoclient.dropdb.php
     * @param string $database
     * @return array
     */
    public function dropDatabase($database)
    {
        if ($this->eventManager->hasListeners(Events::preDropDatabase)) {
            $this->eventManager->dispatchEvent(Events::preDropDatabase, new EventArgs($this, $database));
        }

        $this->initialize();
        $result = $this->riakClient->dropDB($database);

        if ($this->eventManager->hasListeners(Events::postDropDatabase)) {
            $this->eventManager->dispatchEvent(Events::postDropDatabase, new EventArgs($this, $result));
        }

        return $result;
    }

    /**
     * Get the Configuration used by this Connection.
     *
     * @return Configuration
     */
    public function getConfiguration()
    {
        return $this->config;
    }

    /**
     * Get the EventManager used by this Connection.
     *
     * @return \Doctrine\Common\EventManager
     */
    public function getEventManager()
    {
        return $this->eventManager;
    }

    /**
     * Get the RiakClient instance being wrapped.
     *
     * @deprecated 1.1 Replaced by getRiakClient(); will be removed for 2.0
     * @return RiakClient
     */
    public function getRiak()
    {
        return $this->getRiakClient();
    }

    /**
     * Set the RiakClient instance to wrap.
     *
     * @deprecated 1.1 Will be removed for 2.0
     * @param RiakClient $riakClient
     */
    public function setRiak($riakClient)
    {
        if ( ! ($riakClient instanceof RiakClient)) {
            throw new \InvalidArgumentException('RiakClient instance required');
        }

        $this->riakClient = $riakClient;
    }

    /**
     * Get the RiakClient instance being wrapped.
     *
     * @return RiakClient
     */
    public function getRiakClient()
    {
        $this->initialize();
        return $this->riakClient;
    }

    /**
     * Wrapper method for RiakClient::getReadPreference().
     *
     * For driver versions between 1.3.0 and 1.3.3, the return value will be
     * converted for consistency with {@link Connection::setReadPreference()}.
     *
     * @see http://php.net/manual/en/mongoclient.getreadpreference.php
     * @return array
     */
    public function getReadPreference()
    {
        $this->initialize();
        return ReadPreference::convertReadPreference($this->riakClient->getReadPreference());
    }

    /**
     * Wrapper method for RiakClient::setReadPreference().
     *
     * @see http://php.net/manual/en/mongoclient.setreadpreference.php
     * @param string $readPreference
     * @param array  $tags
     * @return boolean
     */
    public function setReadPreference($readPreference, array $tags = null)
    {
        $this->initialize();
        if (isset($tags)) {
            return $this->riakClient->setReadPreference($readPreference, $tags);
        }

        return $this->riakClient->setReadPreference($readPreference);
    }

    /**
     * Get the server string.
     *
     * @return string|null
     */
    public function getServer()
    {
        return $this->server;
    }

    /**
     * Gets the $status property of the wrapped RiakClient instance.
     *
     * @deprecated 1.1 No longer used in driver; Will be removed for 1.2
     * @return string
     */
    public function getStatus()
    {
        $this->initialize();
        if ( ! ($this->riakClient instanceof RiakClient)) {
            return null;
        }

        return $this->riakClient->status;
    }

    /**
     * Construct the wrapped RiakClient instance if necessary.
     *
     * This method will dispatch preConnect and postConnect events.
     */
    public function initialize()
    {
        if ($this->riakClient !== null) {
            return;
        }

        if ($this->eventManager->hasListeners(Events::preConnect)) {
            $this->eventManager->dispatchEvent(Events::preConnect, new EventArgs($this));
        }

        $server = $this->server ?: 'http://localhost:8098';
        $options = $this->options;

        $options = isset($options['timeout']) ? $this->convertConnectTimeout($options) : $options;
        $options = isset($options['wTimeout']) ? $this->convertWriteTimeout($options) : $options;

        $this->riakClient = $this->retry(function() use ($server, $options) {
            return //version_compare(phpversion('mongo'), '1.3.0', '<')
                //? new Riak($server, $options)
                //: 
                new RiakClient($server, $options);
        });

        if ($this->eventManager->hasListeners(Events::postConnect)) {
            $this->eventManager->dispatchEvent(Events::postConnect, new EventArgs($this));
        }
    }

    /**
     * Checks whether the connection is initialized and connected.
     *
     * @return boolean
     */
    public function isConnected()
    {
        if ( ! ($this->riakClient instanceof RiakClient)) {
            return false;
        }

        /* RiakClient::$connected is deprecated in 1.5.0+, so count the list of
         * connected hosts instead.
         */
        return version_compare(phpversion('mongo'), '1.5.0', '<')
            ? $this->riakClient->connected
            : count($this->riakClient->getHosts()) > 0;
    }

    /**
     * Wrapper method for RiakClient::listDBs().
     *
     * @see http://php.net/manual/en/mongoclient.listdbs.php
     * @return array
     */
    public function listDatabases()
    {
        $this->initialize();
        return $this->riakClient->listDBs();
    }

    /**
     * Log something using the configured logger callable (if available).
     *
     * @param array $log
     */
    public function log(array $log)
    {
        if (null !== $this->config->getLoggerCallable()) {
            call_user_func_array($this->config->getLoggerCallable(), array($log));
        }
    }

    /**
     * Wrapper method for RiakClient::selectCollection().
     *
     * @see http://php.net/manual/en/mongoclient.selectcollection.php
     * @param string $db
     * @param string $collection
     * @return Collection
     */
    public function selectCollection($db, $collection)
    {
        $this->initialize();
        return $this->selectDatabase($db)->selectCollection($collection);
    }

    /**
     * Wrapper method for RiakClient::selectDatabase().
     *
     * This method will dispatch preSelectDatabase and postSelectDatabase
     * events.
     *
     * @see http://php.net/manual/en/mongoclient.selectdatabase.php
     * @param string $name
     * @return Database
     */
    public function selectDatabase($name)
    {
        if ($this->eventManager->hasListeners(Events::preSelectDatabase)) {
            $this->eventManager->dispatchEvent(Events::preSelectDatabase, new EventArgs($this, $name));
        }

        $this->initialize();
        $database = $this->doSelectDatabase($name);

        if ($this->eventManager->hasListeners(Events::postSelectDatabase)) {
            $this->eventManager->dispatchEvent(Events::postSelectDatabase, new EventArgs($this, $database));
        }

        return $database;
    }

    /**
     * Wrapper method for RiakClient::__get().
     *
     * @see http://php.net/manual/en/mongoclient.get.php
     * @param string $database
     * @return Riak
     */
    public function __get($database)
    {
        $this->initialize();
        return $this->riakClient->__get($database);
    }

    /**
     * Wrapper method for RiakClient::__toString().
     *
     * @see http://php.net/manual/en/mongoclient.tostring.php
     * @return string
     */
    public function __toString()
    {
        $this->initialize();
        return $this->riakClient->__toString();
    }

    /**
     * Return a new Database instance.
     *
     * If a logger callable was defined, a LoggableDatabase will be returned.
     *
     * @see Connection::selectDatabase()
     * @param string $name
     * @return Database
     */
    protected function doSelectDatabase($name)
    {
        $riak = $this->riakClient->selectDB($name);
        $numRetries = $this->config->getRetryQuery();
        $loggerCallable = $this->config->getLoggerCallable();

        return $loggerCallable !== null
            ? new LoggableDatabase($this, $riak, $this->eventManager, $numRetries, $loggerCallable)
            : new Database($this, $riak, $this->eventManager, $numRetries);
    }

    /**
     * Conditionally retry a closure if it yields an exception.
     *
     * If the closure does not return successfully within the configured number
     * of retries, its first exception will be thrown.
     *
     * @param \Closure $retry
     * @return mixed
     */
    protected function retry(\Closure $retry)
    {
        $numRetries = $this->config->getRetryConnect();

        if ($numRetries < 1) {
            return $retry();
        }

        $firstException = null;

        for ($i = 0; $i <= $numRetries; $i++) {
            try {
                return $retry();
            } catch (RiakException $e) {
                if ($firstException === null) {
                    $firstException = $e;
                }
                if ($i === $numRetries) {
                    throw $firstException;
                }
            }
        }
    }

    /**
     * Converts "timeout" RiakClient constructor option to "connectTimeoutMS"
     * for driver versions 1.4.0+.
     *
     * Note: RiakClient actually allows case-insensitive option names, but
     * we'll only process the canonical version here.
     *
     * @param array $options
     * @return array
     */
    protected function convertConnectTimeout(array $options)
    {
        if (version_compare(phpversion('mongo'), '1.4.0', '<')) {
            return $options;
        }

        if (isset($options['timeout']) && ! isset($options['connectTimeoutMS'])) {
            $options['connectTimeoutMS'] = $options['timeout'];
            unset($options['timeout']);
        }

        return $options;
    }

    /**
     * Converts "wTimeout" RiakClient constructor option to "wTimeoutMS" for
     * driver versions 1.4.0+.
     *
     * Note: RiakClient actually allows case-insensitive option names, but
     * we'll only process the canonical version here.
     *
     * @param array $options
     * @return array
     */
    protected function convertWriteTimeout(array $options)
    {
        if (version_compare(phpversion('mongo'), '1.4.0', '<')) {
            return $options;
        }

        if (isset($options['wTimeout']) && ! isset($options['wTimeoutMS'])) {
            $options['wTimeoutMS'] = $options['wTimeout'];
            unset($options['wTimeout']);
        }

        return $options;
    }
}