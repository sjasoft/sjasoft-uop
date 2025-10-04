# Pointy DB
## Architecture
### Pages
A page is of a fixed size (say 8192 words)
A Page is filled with either fixed size  or variable sized information.
All veriable size information has contents in one or more linked segments each of which has a header specifying size in bytes and pointer to next segment if any. 
## Special Pages
The idea is to have a very linked page type database.  Certain pages are special.
### Index
Index pages start at page 0 always. First page in any sequential PointyDB store must begin with first page of index
### Class/Label 
This is an overlay concept of class definition so non-special Nodes/Objects can be off particular minimal typed attributes
### Attribute/Property
Definitions of particular kind of attribute and other information about it



## Bar
This is ok.  I think.

### Baz

#### Qux

##### Quux

###### Quuux

