package NumberedTree::DBTree;

use strict;

use NumberedTree;

use DBI;

our $VERSION = '1.00';
our @ISA = qw(NumberedTree);

my %collumn_names = (serial => 'serial', 
		      parent => 'parent', 
		      name => 'name');

# <getStatements> a service method that prepares SQL statements used in the 
# module.
# Arguments: $colnames - names of the columns that make up the table (see
#            %collumn_names above).
# Returns: a hash with the statements.

sub getStatements {
    my ($self, $cols) = @_;
    $cols ||= \%collumn_names;

    my $dbh = $self->{Source};
    my $table = $self->{SourceName};

    my %statements;
    $statements{add} = "insert into $table set " . 
	"$cols->{parent}=$self->{_Parent}, " . 
	"$cols->{name}=?";
    $statements{who} = "select max($cols->{serial}) from $table";
    $statements{delete} = "delete from $table where $cols->{serial}=?";
    $statements{setValue} = "update $table set $cols->{name}=? " .
	"where $cols->{serial}=?"; # We don't have a number yet...
    $statements{truncate} = "truncate $table";

    $self->{Statements} = {map {$_ => $dbh->prepare($statements{$_}) } 
			   keys %statements};
}

# <new> constructs a new tree or node.
# Arguments: By name: 
#            value - the value to be stored in the node.
#            source_name - table name (DB) or file name (XML).
#            source - DB handle. A twig will be created automaticaly for XML.
# Returns: The tree object.

sub new {
    my $parent = shift;
    my %args = @_;

    my $parent_serial;
    my $class;
    
    my $properties = $parent->SUPER::new($args{value});

    if ($class = ref($parent)) {
	$properties->{Source} = $parent->{Source};   
	$properties->{SourceName} = $parent->{SourceName};
	$properties->{_Parent} = $parent->{_Serial};
    } else {
	# Check that args are correct:
	unless ($args{source_name} && $args{source}){
	    warn "No source or name, failed to create a tree";
	    return undef; 
	}

	$class = $parent;
	$properties->{_Parent} = $args{parent} ||= 0;
	$properties->{Source} = $args{source} ||= '';   
	$properties->{SourceName} = $args{source_name} ||= '';
    }

    $properties->getStatements($args{column_names});
    $properties->{_Serial} = addNodeDB($properties) unless $args{NoWrite};

    return $properties;
}

# <addNodeDB> adds a record to a table containing a tree.
# Arguments: $self - the node's hash. This is not a class method because the 
#            object we're operating on is not yet blessed.
# Returns: The new Serial number of the Item added.

sub addNodeDB {
    my $self = shift;
    my $parent = $self->{_Parent};
    my $value = $self->{Value};

    $self->{Statements}->{add}->execute($value) or return undef;
    $self->{Statements}->{who}->execute;
    return ($self->{Statements}->{who}->fetchrow_array)[0];
}

# <readDB> constructs a new tree from a pre-existing table.
# Arguments: $table - table name.
#            $dbh - database handle to operate on.
#            $cols - a hash giving alternative collumn names.
# Returns: The tree object.

sub read {
    my ($self, $table, $dbh, $cols) = @_;
    return undef if (ref $self);

    $cols ||= \%collumn_names;

    my @parents = @{
	$dbh->selectall_arrayref("select parent from $table group by parent")
	};
    # The prntnums hash is used to save calls to the DB. 
    # if a row is not a parent,
    # there's no need to query the database about its childs.
    my %prntnums = map {$_->[0] => 1} @parents;
    delete $prntnums{0}; # or endless recursion...

    my $sth = $dbh->prepare("select serial, name from $table " . 
			    "where $cols->{parent} = ?");

    $sth->execute(0);
    my $root = $sth->fetchrow_arrayref;

    # Start construction of root element:
    my $tree = NumberedTree::DBTree->new(value => $root->[1],
					 source => $dbh,
					 source_name => $table,
					 parent => 0,
					 NoWrite => 1);
    $tree->{_Serial} = $root->[0];
    $tree->recursiveTreeBuild($sth, %prntnums);

    return $tree;
}

sub recursiveTreeBuild {
    my ($self, $sth, %prntnums) = @_;
    my $serial = $self->{_Serial};

    $sth->execute(($serial));
    my @rows = @{$sth->fetchall_arrayref};
    
    foreach my $row (@rows) {
	# 0 - serial, 1 - name.
	my $newNode = $self->append($row->[1], 1);
	$newNode->{_Serial} = $row->[0];
	next unless (delete $prntnums{$row->[0]});
	
	$newNode->recursiveTreeBuild($sth, %prntnums);
    }
}

# <append> adds a node to the tree, at the end of the Items array.
# Arguments: $noWrite - if supplied and true, the new node will not be written 
#            to the DB. this is useful when you are constructing a tree out of 
#            existing data.
# Returns: The new node.

sub append {
    my $self = shift;
    my ($value, $noWrite) = @_;

    my $newNode = $self->new(value => $value, 
			     NoWrite => $noWrite);
    return undef unless $newNode;

    push @{$self->{Items}}, $newNode;
    return $newNode;
}

# <delete> deletes the item pointed to by the cursor.
# The curser is not changed, which means it effectively moves to the next item.
# However it does change to be just after the end if it is already there,
# so you won't get an overflow.
# Arguments: None.
# Returns: The deleted item or undef if none was deleted.
#          Note that the returned item is invalid since it's deleted from its 
#          table.

sub delete {
    my $self = shift;
    my $deleted = $self->SUPER::delete;

    if ($deleted) { 
	$deleted->{Statements}->{delete}->execute($deleted->{_Serial}); 
    }
    return $deleted;
}

sub setValue {
    my $self = shift;
    $self->{Value} = shift;
    $self->{Statements}->{setValue}->execute($self->{Value}, $self->{_Serial});
}

# <truncate> removes the entire table tied to the tree.
# Arguments: None.
# Returns: Nothing.

sub truncate {
    my $self = shift;
    $self->{Statements}->{truncate}->execute;
    $self = undef;
}

# <revert> re-blesses the tree into the parent class, losing DB Tie.
# Arguments: None.
# Returns: Nothing.

sub revert {
    my $self = shift;
    my $keep_data = shift;

    # Remove data specific to this class:
    unless ($keep_data) {
	delete $self->{Source};
	delete $self->{SourceName};
	delete $self->{Statements};
    }
    $_->revert foreach (@{ $self->{Items} });

    return bless $self, $ISA[0];
}

1;

=head1 NAME

 NumberedTree::DBTree - a NumberedTree that is tied to a DB table.

=head1 SYNOPSIS

  use NumberedTree::DBTree;
  my $dbh = DBI->connect(...);

  # The easy way:
  my $tree = NumberedTree::DBTree->read($table, $dbh);

  # The hard way:
  my $tree = NumberedTree::DBTree->new(source_name => 'a_table', 
                                       source => $dbh);
  while (I aint sick of it) {
  	$tree->append($newValue);
  }
  
  etc.
  
=head1 DESCRIPTION

DBTree is a child class of NumberedTree.pm that supplies database tying (every change is immediately reflected in the database) and reading using tables that are built to store a tree (the structure is described below). It's basically the same as NumberedTree except for some methods. These, and arguments changes for inherited methods, are also described below. For the rest, please refer to the documentation for NumberedTree.pm.

=head1 CREATING A TABLE FOR THE TREE

A table used by this module must have at least 3 columns: the serial number column (by default 'serial'), the parent column (default - 'parent') and the value column (default - 'name'). If the default names don't suit you, don't worry - you can supply different names to the constructors). Serial numbers start from any number greater than zero and must be auto increment fields. Parent numbers of course are the serial numbers of the parent for each node - the root node B<always> takes parent number 0.

 Example SQL statement to build the table:  
 create table places (serial int auto_increment primary key, 
 				  parent int not null, 
				  name varchar(20));

=head1 METHODS

This section only describes methods that are not the same as in NumberedTree.

=head2 Constructors

There are now two of them:

=over 4

=item new (source => I<source>, source_name => I<source_name>)

creates a new tree object that uses an empty table named E<lt>I<source_name>E<gt> using a database handle supplied via the I<source> argument.

=item read (SOURCE_NAME, SOURCE);

creates a new tree object from a table that contains tree data as specified above. Arguments are the same as to I<new>.

=back

=head2 Other methods

Two methods are added to this class:

=over 4

=item truncate

Activates the truncate SQL command, effectively deleting all data in a tree, but not the table. This also disposes of the tree object, so you'll have to build a new one after using this method.

=item revert

Removes information that is specific to this class and re-blesses the entire tree into the parent class.

=back

=head1 BUGS

Please report through CPAN: 
E<lt>http://rt.cpan.org/NoAuth/Bugs.html?Dist=NumberedTreeE<gt>
or send mail to E<lt>bug-NumberedTree#rt.cpan.orgE<gt> 

=head1 SEE ALSO

NumberedTree.pm

=head1 AUTHOR

Yosef Meller, E<lt>mellerf@netvision.net.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2003 by Yosef Meller

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself. 

=cut
