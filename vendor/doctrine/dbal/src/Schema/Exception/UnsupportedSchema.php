<?php

declare(strict_types=1);

namespace Doctrine\DBAL\Schema\Exception;

use Doctrine\DBAL\Schema\SchemaException;
use RuntimeException;

use function sprintf;

final class UnsupportedSchema extends RuntimeException implements SchemaException
{
    public static function sqliteMissingForeignKeyConstraintReferencedColumns(
        string $constraintTableName,
        string $referencingTableName,
    ): self {
        return new self(sprintf(
            'Unable to introspect foreign key constraint "%s" because the referenced column names are omitted,'
                . ' and the referenced table "%s" does not exist.',
            $constraintTableName,
            $referencingTableName,
        ));
    }
}
