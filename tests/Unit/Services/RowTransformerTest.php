<?php

namespace Andach\ExtractAndTransform\Tests\Unit\Services;

use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Services\RowTransformer;
use Andach\ExtractAndTransform\Tests\TestCase;

class RowTransformerTest extends TestCase
{
    private RowTransformer $transformer;

    protected function setUp(): void
    {
        parent::setUp();
        $this->transformer = new RowTransformer;
    }

    public function test_transform_returns_row_as_is_when_mapping_is_empty(): void
    {
        $row = ['id' => 1, 'name' => 'test'];
        $result = $this->transformer->transform($row, []);
        $this->assertEquals($row, $result);
    }

    public function test_transform_renames_columns_based_on_mapping(): void
    {
        $row = ['id' => 1, 'name' => 'test'];
        $mapping = ['id' => 'remote_id', 'name' => 'full_name'];

        $result = $this->transformer->transform($row, $mapping);

        $this->assertEquals(['remote_id' => 1, 'full_name' => 'test'], $result);
    }

    public function test_transform_excludes_columns_mapped_to_null(): void
    {
        $row = ['id' => 1, 'name' => 'test', 'secret' => 'hidden'];
        $mapping = ['id' => 'id', 'name' => 'name', 'secret' => null];

        $result = $this->transformer->transform($row, $mapping);

        $this->assertEquals(['id' => 1, 'name' => 'test'], $result);
        $this->assertArrayNotHasKey('secret', $result);
    }

    public function test_transform_excludes_unmapped_columns_when_mapping_exists(): void
    {
        $row = ['id' => 1, 'name' => 'test', 'extra' => 'data'];
        $mapping = ['id' => 'id', 'name' => 'name'];

        $result = $this->transformer->transform($row, $mapping);

        $this->assertEquals(['id' => 1, 'name' => 'test'], $result);
        $this->assertArrayNotHasKey('extra', $result);
    }

    public function test_filter_columns_returns_fields_as_is_when_mapping_is_empty(): void
    {
        $fields = [new RemoteField('id', 'int', false, 'int')];
        $result = $this->transformer->filterColumns($fields, []);
        $this->assertEquals($fields, $result);
    }

    public function test_filter_columns_renames_and_excludes_fields(): void
    {
        $fields = [
            new RemoteField('id', 'int', false, 'int'),
            new RemoteField('secret', 'string', true, 'string'),
        ];
        $mapping = ['id' => 'remote_id', 'secret' => null];

        $result = $this->transformer->filterColumns($fields, $mapping);

        $this->assertCount(1, $result);
        $this->assertEquals('remote_id', $result[0]->name);
    }
}
