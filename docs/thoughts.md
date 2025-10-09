# Language Musing
## Language idea
Consider any piece of software of more than very small or primitive link.  This program can be thought of as a set of relatively coherent chunks.  We may find these chunks in different ways.  For instance, from the perspective of logical user interaction chunks (or user intentions) or the perspective of logical computational chunks of work.  

Both representations are hierachical - "chunks" may contain other "chunks". Of course such chunks can trivially be expressed in sexpr and other notations.   Note any piece of information may be presented to the user in variable ways by medium of presentation.  Or if you like that we can have multiple views on an specific piece and any type of information. So a generic UI display language would talk in terms of data elements and logical UI view comonent identification and modification descriptions (size, css, etc.)  But they are all just coherent (hopefully) to some context presentations of part of the information in the data.  

Another representation is in terms of chunks or writing programs in general and exposing the information from them in various media or producing various functional conversions of input streams to output streams.  Remembering select previous inputs and outputs means they may be used for state and to change subsequent conversions of inputs to outputs.  

Despite the theoretical reasons for avoiding state we see that most continuous systems including organizations and organism do retain state and are shaped by over time.

I think we are better so far at capturing chunking in programs, not so good at chunking translanguage and problem, and not very good at chunking user interface interactions and intentions.   If we were better at the last then could conceive of a UI language that would be interpreted in the context of various display devices including desktop, tablet, phone, text.

## UOP Language

### remember (<string>)
Remembers whatever the string describes.  For an url it is a callable on that url.  Information is fetched from the url.  

For all things remembered the user is present with a some matter of display of what the system knows about that particular thing.  This display does not show associated things in its standard state but can be expanded to explore reachable context around the object along different dimensions.

### find (<string>)

#### *Sting format

##### For remember
An input string may partically specify a type of object or be a effective locator or indicaton (id) of that which is to be found or remembered.  Remember is for remembering a single object or group of objects and some related information such as tags relations to other objects and so on.

###### URI
An URL or a filepath or some random Universal  Resource Identifier.
In this form the knowledge item is the page, file, video, or knowledge item at large referred to by the address-like string or identifier.
###### Type(<common_representation>)
This is a type of thing and some idenitifer of a particular instance or group of instances.  E.g., Person(John Smith).
###### Description
This is sort of like the previous input string but more explicit such as Person(:first_name first :last_name last).
But a description can also include various kinds of associated information such as tags, relationships to other objects and groups the object[s] is/are in.

##### For find
Find locates and displays one or more objects.  In the case of a single object it displays that objects data and a way to see its associated knowledge items.  In the case of more than one object it displays the list of objects and their main attributes and ways to sort and group by those attributes.    Selecting an item expands it as for one item.

###### Description
Description is extended to include general queries combining conditions on type, properties, tags, relationships, groups, timestamp range.

##
