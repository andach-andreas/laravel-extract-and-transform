<?php

namespace Andach\ExtractAndTransform\Tests\Unit\Connectors;

use Andach\ExtractAndTransform\Connectors\BaseConnector;
use Andach\ExtractAndTransform\Tests\TestCase;
use LogicException;
use Mockery;

class BaseConnectorTest extends TestCase
{
    private BaseConnector $connector;

    protected function setUp(): void
    {
        parent::setUp();
        // Create a partial mock of the abstract class.
        // This allows us to test the concrete methods while leaving the abstract ones unimplemented.
        $this->connector = Mockery::mock(BaseConnector::class)->makePartial();
    }

    public function test_stream_rows_throws_exception_test(): void
    {
        $this->expectException(LogicException::class);
        iterator_to_array($this->connector->streamRows(new \Andach\ExtractAndTransform\Data\RemoteDataset('test', 'test'), []));
    }

    public function test_infer_schema_throws_exception_test(): void
    {
        $this->expectException(LogicException::class);
        $this->connector->inferSchema(new \Andach\ExtractAndTransform\Data\RemoteDataset('test', 'test'), []);
    }

    public function test_list_identities_throws_exception_test(): void
    {
        $this->expectException(LogicException::class);
        iterator_to_array($this->connector->listIdentities(new \Andach\ExtractAndTransform\Data\RemoteDataset('test', 'test'), [], ['id']));
    }

    public function test_stream_rows_with_checkpoint_throws_exception_test(): void
    {
        $this->expectException(LogicException::class);
        iterator_to_array($this->connector->streamRowsWithCheckpoint(new \Andach\ExtractAndTransform\Data\RemoteDataset('test', 'test'), [], null));
    }
}
