# ExpressionTree

A container that holds arithmetic or logical expressions.

## 1. Motivation

The main goal of the ExpressionTree is to effectively store a tree-like structure of parsed expression and to effectively build this structure during sequential parsing.

## 2. Description

```c++
template <typename OperationType, typename SubTree, int holdSize, typename... Ts>
class ExpressionTree;
```
* `OperationType` - a type that represents the expression operators (arithmetic or logical)
* `SubTree` - a type that represents a head of subtree (subexpression)
* `holdSize` - count of nodes that should be stored on the stack without allocations
* `Ts...` - types of the expression arguments.

`ExpressionTree` does not support operator precedence.
You can support it manually as it done in `QueryEntries` and `SelectIteratorContainer`, or by enclosing higher priority operators in brackets as it done in `SortExpression`.
Here is not used the traditional way for constructing trees with inheritance of nodes, allocations of separate nodes and holding  pointers to them.
`ExpressionTree` holds all nodes by value in a vector (`container_`) sequentially in type `Node` based on `variant`.

Subtree is stored in `container_` just behind its head (`SubTree`) which holds occupied space. For details see examples.
This architecture allows to reduce count of allocations and virtual functions calls.

### 2.1. Examples

Expression
```
A + (B - C - (D + E)) - F
```
would be stored like

|  0   |  1   |  2   |  3   |  4   |  5   |  6   |  7   |
|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|
| +, A | +, 6 | +, B | -, C | -, 3 | +, D | +, E | -, F |

Every cell holds an operator (here `+` or `-`) and payload.

Lets see cells 4-5: they correspond to subexpression `-(D + E)`.
* Cell 4 is a head of subtree: `-` - operator before the bracket and `3` - count of cells occupied by subtree (from 4 to 6).
* Cell 5 holds default operator `+` and value `D`.
* Cell 6 holds operator `+` and value `E`.

Similarly, cell 1 stores `6` - count of cells occupied by subtree (from 1 to 6).

This structure provides effective forward iteration and doesn't allow to iterate backwards.

Default building process of ExpressionTree is to append elements from beginning to end by using methods `Append()`, `OpenBracket()` and `CloseBracket()`.
`activeBrackets_` - list of cells of heads of open brackets which sizes should be incremented when new element is added.
Lets trace evolution of `container_` and `activeBrackets_` during construction expression `(A + B) - (C + (D - E) + F) - G`:
1. `OpenBracket(+)`. Constructed expression: `(`

|  0   |
|:----:|
| +, 1 |

|  0  |
|:---:|

2. `Append(+, A)`. Constructed expression: `(A`

|    0     |  1   |
|:--------:|:----:|
| +, **2** | +, A |

|  0  |
|:---:|

3. `Append(+, B)`. Constructed expression: `(A + B`

|    0     |  1   |  2   |
|:--------:|:----:|:----:|
| +, **3** | +, A | +, B |

|  0  |
|:---:|

4. `CloseBracket()`. Constructed expression: `(A + B)`

|  0   |  1   |  2   |
|:----:|:----:|:----:|
| +, 3 | +, A | +, B |

`activeBrackets_` is empty

5. `OpenBracket(-)`. Constructed expression: `(A + B) - (`

|  0   |  1   |  2   |  3   |
|:----:|:----:|:----:|:----:|
| +, 3 | +, A | +, B | -, 1 |

|  3  |
|:---:|

6. `Append(+, C)`. Constructed expression: `(A + B) - (C`

|  0   |  1   |  2   |    3     |  4   |
|:----:|:----:|:----:|:--------:|:----:|
| +, 3 | +, A | +, B | -, **2** | +, C |

|  3  |
|:---:|

7. `OpenBracket(+)`. Constructed expression: `(A + B) - (C + (`

|  0   |  1   |  2   |    3     |  4   |  5   |
|:----:|:----:|:----:|:--------:|:----:|:----:|
| +, 3 | +, A | +, B | -, **3** | +, C | +, 1 |

|  3  |  5  |
|:---:|:---:|

8. `Append(+, D)`. Constructed expression: `(A + B) - (C + (D`

|  0   |  1   |  2   |    3     |  4   |    5     |  6   |
|:----:|:----:|:----:|:--------:|:----:|:--------:|:----:|
| +, 3 | +, A | +, B | -, **4** | +, C | +, **2** | +, D |

|  3  |  5  |
|:---:|:---:|

9. `Append(-, E)`. Constructed expression: `(A + B) - (C + (D - E`

|  0   |  1   |  2   |    3     |  4   |    5     |  6   |  7   |
|:----:|:----:|:----:|:--------:|:----:|:--------:|:----:|:----:|
| +, 3 | +, A | +, B | -, **5** | +, C | +, **3** | +, D | -, E |

|  3  |  5  |
|:---:|:---:|

10. `CloseBracket()`. Constructed expression: `(A + B) - (C + (D - E)`

|  0   |  1   |  2   |  3   |  4   |  5   |  6   |  7   |
|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|
| +, 3 | +, A | +, B | -, 5 | +, C | +, 3 | +, D | -, E |

|  3  |
|:---:|

11. `Append(+, F)`. Constructed expression: `(A + B) - (C + (D - E) + F`

|  0   |  1   |  2   |    3     |  4   |  5   |  6   |  7   |  8   |
|:----:|:----:|:----:|:--------:|:----:|:----:|:----:|:----:|:----:|
| +, 3 | +, A | +, B | -, **6** | +, C | +, 3 | +, D | -, E | +, F |

|  3  |
|:---:|

12. `CloseBracket()`. Constructed expression: `(A + B) - (C + (D - E) + F)`

|  0   |  1   |  2   |  3   |  4   |  5   |  6   |  7   |  8   |
|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|
| +, 3 | +, A | +, B | -, 6 | +, C | +, 3 | +, D | -, E | +, F |

`activeBrackets_` is empty

13. `Append(-, G)`. Constructed expression: `(A + B) - (C + (D - E) + F) - G`

|  0   |  1   |  2   |  3   |  4   |  5   |  6   |  7   |  8   |  9   |
|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|
| +, 3 | +, A | +, B | -, 6 | +, C | +, 3 | +, D | -, E | +, F | -, G |

`activeBrackets_` is empty

## 3. `class Bracket`

Default type for `SubTree` template argument. Just holds size of the subtree that is incremented by `Append()` and is reduced by `Erase()`.


## 4. `class ExpressionTree::Node`

`Node` is type of `container_`'s cells.
It contains operation (value of `OperationType`) and a value of one of the types:
- `SubTree` (`Bracket` by default) if it is head of subexpression.
- one of `Ts...` types.

### 4.1. Methods

- `T& Node::Value<T>()`, `const T& Node::Value<T>() const` return reference to payload value if it holds value of type `T`, fail otherwise.
- `size_t Node::Size() const` returns `1` if it is not head of subexpression or count of cells occupied by subexpression otherwise.
- `bool Node::IsSubTree() const`, `bool Node::IsLeaf() const` test is the `Node` head of subexpression or vice versa.
- `bool Node::Holds<T>() const` returns `true` if it holds value of type `T`.
- `void Node::Append()` increments size of subexpression if it is head of subexpression, fails otherwise.
- `void Node::Erase(size_t)` reduces size of subexpression if it is head of subexpression, fails otherwise.
- invokes `visitor` with the payload as argument:
```c++
  template <typename Visitor>
  void Node::Visit(Visitor&& visitor);
  template <typename Visitor>
  void Node::Visit(Visitor&& visit) const;
  ```

- `Node Node::Copy() const &` returns copy of origin one if it is head of subexpression or holds value of one of `Ts...` types.
- `Node Node::Move() &&` returns move-copy of origin one if it is head of subexpression or holds value of one of `Ts...` types.
- `void Node::SetValue<T>(T&&)` sets the `Node` to hold new value of type `T`.
- `void Node::Emplace<T, Args...>(Args&&...)` sets the `Node` to hold new value of type `T`, constucting it with `Args...`.

## 5. `class ExpressionTree::iterator` and `class ExpressionTree::const_iterator`

They are forward iterators which iterates over nodes of one level and do not go into subexpressions.
So if an iterator `it` points not to head of a subexpression after operation `++it` it will point to next cell.
And if an iterator `it` points to head of a subexpression after operation `++it` it will point to the cell next after the last cell of the subexpression.
To iterate into subexpression use methods

```c++
iterator iterator::begin();
const_iterator iterator::cbegin();
const_iterator const_iterator::begin();
const_iterator const_iterator::cbegin();
iterator iterator::end();
const_iterator iterator::cend();
const_iterator const_iterator::end());
const_iterator const_iterator::cend();
```
if the iterator points to head of subexpression these methods return an iterator that points to the first or next after the last cell of the subexpression or fail otherwise.

For example, for expression `A + B - (C - D + (E - F) - G)`

|  0   |  1   |  2   |  3   |  4   |  5   |  6   |  7   |  8   |
|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|
| +, A | +, B | -, 7 | +, C | -, D | +, 3 | +, E | -, F | -, G |

- if `it` points to cell `1` when `it.begin()` (and similar) fails and after `++it` it will point to cell `2`.
- if `it` points to cell `2` when `it.begin()` returns an iterator that points to cell `3`, `it.end()` returns an iterator that points to cell after `8` and `++it` makes it to point to cell after `8`.
- if `it` points to cell `3` when `it.begin()` (and similar) fails and after `++it` it will point to cell `4`.
- if `it` points to cell `5` when `it.begin()` returns an iterator that points to cell `6`, `it.end()` returns an iterator that points to cell `8` and `++it` makes it to point to cell `8`.

`iterator` can be converted to `const_iterator` but not vice versa.

## 6. Methods

- Copy constructor and copy assignment operator make deep copy for all copying nodes.
- Get iterators, which point to the first or next after the last cell of the expression:
```c++
iterator ExpressionTree::begin();
const_iterator ExpressionTree::begin() const;
const_iterator ExpressionTree::cbegin() const;
iterator ExpressionTree::end();
const_iterator ExpressionTree::end() const;
const_iterator ExpressionTree::cend() const;
```
- Get iterator that points to the first cell of current active subexpression (last subexpression for which `OpenBracket()` was called and `CloseBracket()` was not) and returns `begin()` if no active subexpression:
```c++
const_iterator ExpressionTree::begin_of_current_bracket() const
```
- Append an operand to the end or to the beginning of the expression. `T` must be one of `Ts...` types. Returns actual number of the inserted entries (method may create additional entries during the postprocessing):
```c++
size_t ExpressionTree::Append<T>(OperationType, const T&);
size_t ExpressionTree::Append<T>(OperationType, T&&);
size_t ExpressionTree::AppendFront<T>(OperationType, T&&);
```
- Append deep copy of a part of another expression:
```c++
void ExpressionTree::Append(const_iterator begin, const_iterator end);
```
  Inserts/emplaces new element on the requested position. Returns actual number of the inserted entries (method may create additional entries during the postprocessing):
```c++
size_t ExpressionTree::Insert<T>(size_t pos, OperationType, T&&);
size_t ExpressionTree::Emplace<T, Args...>(size_t pos, OperationType, T&&, Args&&...);
```
  Inserts new element after the requested position. Returns actual number of the inserted entries (method may create additional entries during the postprocessing):
```c++
size_t ExpressionTree::InsertAfter<T>(size_t pos, OperationType, T&&);
```
- Start and finish subexpression. `args...` are forwarded to constructor of `SubTree`:
```c++
void ExpressionTree::OpenBracket<Args...>(OperationType, Args&&... args);
void ExpressionTree::CloseBracket();
```
- Get or set operation of node in cell number `i`:
```c++
OperationType ExpressionTree::GetOperation(size_t i) const ;
void ExpressionTree::SetOperation(OperationType op, size_t i);
```
- `void ExpressionTree::SetLastOperation(OperationType)` - set operation to last appended leaf or last closed subtree or last open subtree if it is empty.
- `bool ExpressionTree::Empty() const` - test if the expression empty.
- `size_t ExpressionTree::Size() const` - get count of cells the expression occupies.
- `size_t ExpressionTree::Size(size_t i) const` - get count of cells subexpression occupies if cell `i` is head of the subexpression or `1` otherwise.
- `bool ExpressionTree::IsValue(size_t i) const` - test if the cell `i` is not head of a subexpression.
- `void ExpressionTree::Erase(size_t from, size_t to)` - remove nodes with indexes from `from` to `to - 1`.
- `size_t ExpressionTree::Next(size_t i) const` - get index of cell after the last cell of subexpression if cell `i` is head of the subexpression or `i + 1` otherwise. For example, for expression `A + B - (C - D + (E - F) - G)`
	|  0   |  1   |  2   |  3   |  4   |  5   |  6   |  7   |  8   |
	|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|
	| +, A | +, B | -, 7 | +, C | -, D | +, 3 | +, E | -, F | -, G |

	-# `Next(1)` returns `2`.
	-# `Next(2)` returns `9`.
	-# `Next(3)` returns `4`.
	-# `Next(5)` returns `8`.
- Invokes `Visitor` with payload of node number `i` as argument:
```c++
template <typename Visitor>
void ExpressionTree::Visit(size_t i, Visitor&&) const;
template <typename Visitor>
void ExpressionTree::Visit(size_t i, Visitor&&);
```
- Invokes `Visitor` for each node with its payload as argument:
```c++
template <typename Visitor>
void ExpressionTree::VisitForEach(const Visitor&) const;
template <typename Visitor>
void ExpressionTree::VisitForEach(const Visitor&);
```
  Tries to update entry with new values without new entries creation if possible (). Returns 'true' on success. Returns old values in 'values' param on success:
```c++
bool TryUpdateInplace(size_t pos, U& values);
```
